# booking — 예매·결제 서비스 (Java / Spring Boot)

queue가 정원 게이트로 통과시킨 사용자에 대해 결제를 처리하고 좌석 예매를 확정하는 서비스. queue와 처리 성격이 반대여서(요청 폭주 대신 트랜잭션 정합성) 저장소와 런타임도 다르게 구성한다.

- 역할: 좌석 선점 · 결제(PG mock) · 예매 확정 · 이중판매 차단
- 저장소: MySQL(확정 예매, 영속) + Redis(좌석락·입장 인증)
- 통신: 클라이언트 ↔ REST / queue ↔ Kafka 세 토픽
- 런타임: JVM 21, 가상 스레드

---

## 시스템에서의 위치

```
   브라우저
     │
     ▼
  frontend (정적 SPA / 클러스터에선 Ingress가 경로 분기)
     │  /api/movies · /api/screenings · /api/seats · /api/bookings
     ▼
 ┌──────────────┐  ◀── admissions         (입장 승인) ─── queue
 │   booking    │  ◀── admissions-revoked (입장 회수) ───
 │  (이 서비스) │  ─── bookings-completed (자리 반환) ──▶
 └───┬──────┬───┘
     │      │
     ▼      ▼
  MySQL   Redis
 (확정)  (좌석락·admitted)
```

booking의 예매 관련 동작은 전부 **입장 인증 게이트**를 통과해야 한다. 인증은 booking이 스스로 만들지 않는다 — queue가 승격하면 `admissions`로 받아 발급하고, queue가 자리를 거두면 `admissions-revoked`로 지운다. 예매가 끝나면 `bookings-completed`를 발행해 queue가 자리를 반환하게 한다(닫힌 순환).

정원 통제의 소유자가 queue 한 곳이라, booking은 "지금 몇 명까지 받아도 되나"를 스스로 판단하지 않는다.

---

## 입장 인증(admitted)

**표현은 "멤버당 키 1개 + TTL"이다.** Set(SADD/SISMEMBER)이 아니다.

```
키   admitted:{movieId}:{requestId}      값 "1"      TTL = ADMITTED_TTL (기본 180초)
```

Set을 쓰면 멤버별 TTL이 없어 만료를 Redis가 대신해줄 수 없다. 그러면 인증을 지우는 경로가 예매 확정 성공 한 곳뿐이라, 세션 만료·이탈로 자리를 잃은 사용자의 인증이 계속 남고 Set도 무한히 자란다. 멤버당 키로 두면 만료가 Redis 자체 기능이 되고, 키 개수 상한이 곧 입장 정원이라 유계다.

인증이 사라지는 경로는 셋이다.

| 경로 | 계기 | 성격 |
|---|---|---|
| 즉시 회수 | `admissions-revoked` 수신 | queue가 자리를 거둔 시점에 바로 반영 |
| 소진 | 예매 확정 성공 | 재입장 전 재예매 방지 |
| TTL 만료 | 시간 경과 | 회수 이벤트를 놓쳤을 때의 최후 방어 |

**`ADMITTED_TTL`은 queue의 `SESSION_TIMEOUT`보다 길어야 한다.** 짧으면 정상 세션이 진행 중에 인증을 잃어 좌석선택이 403이 된다. TTL은 이벤트가 유실됐을 때만 동작하는 안전망이고, 정상 경로의 반영은 회수 이벤트가 담당한다.

---

## 요청 흐름 — `POST /api/bookings` 한 요청

`BookingService.confirm()`이 오케스트레이션한다. 트랜잭션(DB)만 별도 빈으로 분리돼 있다.

```
 요청: { screeningId, seatNos, requestId, idempotencyKey }
   │
   ├─ ① 요청 정규화 ────────────────────────────────────────
   │    중복 제거 · 정렬 · 개수 상한(MAX_SEATS_PER_REQUEST)
   │
   ├─ ② 멱등 ───────────────────────────────────────────────
   │    idempotency_key 존재? → 기존 결과 반환(재결제 안 함)
   │
   ├─ ③ 게이트 ─────────────────────────────────────────────
   │    입장 인증 있나?  없으면 403
   │
   ├─ ④ 좌석 실재·판매완료 확인 ────────────────────────────
   │    없는 좌석 → 400 / 이미 팔린 좌석 → 409
   │
   ├─ ⑤ 락 재확인·갱신 ─────────────────────────────────────
   │    renewMine(내 좌석 TTL 연장)   실패 → 409
   │
   ├─ ⑥ PG 승인 (동기, 트랜잭션 밖) ────────────────────────
   │    pg.approve()  →  돈 빠짐 (mock)
   │
   ├─ ⑦ 확정 (트랜잭션, timeout 5s) ────────────────────────
   │    INSERT bookings + booking_seats (좌석번호 정렬 순서로)
   │      UNIQUE(screening,seat) 위반 → 롤백 → 멱등키 재조회 → 없으면 환불
   │      기타 커밋 실패 → 롤백 → 환불
   │
   └─ ⑧ 후처리 (커밋 후, best-effort) ──────────────────────
        락 해제 · bookings-completed 발행 · 인증 소진
        (실패해도 로그만 — 예매는 이미 커밋됨)
```

**멱등이 게이트보다 앞이다.** 확정에 성공하면 인증이 소진되므로, 게이트가 앞에 있으면 "결제는 됐는데 응답이 유실돼 클라이언트가 같은 멱등키로 재시도"하는 경로가 403으로 막힌다 — 돈은 빠졌는데 예매번호를 못 받고, 지표에도 replay가 아니라 forbidden으로 찍힌다. 멱등키 조회는 상태를 바꾸지 않고, 키는 요청자만 아는 값이며, 회차·좌석이 다르면 뒤에서 409로 거절한다.

**좌석 실재 확인이 PG 승인보다 앞이다.** `booking_seats`에는 `seats`로의 FK가 없어(판매완료를 행 존재로 판정하는 구조), 검증이 없으면 존재하지 않는 좌석번호로 점유·결제·확정이 전부 통과하고 그 행이 잔여좌석을 깎는다.

**PG 호출은 트랜잭션 밖이다.** 느린 외부 결제가 DB 커넥션을 붙잡지 않게 하기 위해서다. `confirm()`(트랜잭션 없음)이 `persist()`(트랜잭션 있음)를 별도 빈으로 호출해 프록시를 통과시킨다.

**좌석 INSERT 순서를 정렬로 고정한다.** `[A1,A2]`와 `[A2,A1]`이 동시에 들어오면 서로 상대가 잡은 행을 기다려 InnoDB 데드락이 된다. 트랜잭션 타임아웃 5초는 그 대기가 `innodb_lock_wait_timeout`(기본 50초)까지 커넥션을 붙잡는 것을 막는다.

---

## 이중판매 방어 — 락은 필터, UNIQUE는 보증

| 층 | 수단 | 성격 |
|---|---|---|
| 1차 필터 | Redis `SET NX`(좌석락) | 동시 요청을 Redis 단일 스레드가 직렬 처리해 대부분 조기 차단. TTL 만료·failover에 뚫릴 수 있음 |
| 판매완료 확인 | MySQL `booking_seats` 조회 | 확정 직후 락이 해제되므로 이미 팔린 좌석은 락으로 안 걸린다. PG 승인 전에 확인한다 |
| 최종 보증 | MySQL `UNIQUE(screening, seat)` | 위 둘이 뚫려도 커밋 순간 원자로 두 번째를 거부(진 쪽은 롤백+환불) |

트랜잭션은 동시성 방어가 아니라 정합(절반 반영 방지)을 담당하고, 동시성은 UNIQUE가 막는다.

---

## 좌석 상태 3개

```
   빈자리        ─ 어느 쪽에도 없음
   임시점유      ─ Redis  seat:{screeningId}:{seatNo}   (SET NX EX, TTL 45s)
   판매완료      ─ MySQL  booking_seats 행 존재          (영구)

   화면 응답 taken = (판매완료 OR 임시점유)
```

임시점유는 TTL로 사라져 다시 빈자리가 되지만 판매완료는 영구다. 잔여 좌석은 반드시 두 출처를 합산한다(`total − 판매완료 − 임시점유`).

**점유 좌석은 회차별 인덱스 Set으로 센다.** `seatlocks:{screeningId}`에 점유 중인 좌석번호를 좌석락과 같은 Lua 안에서 함께 기록한다. 인덱스가 없으면 "이 회차에 몇 석 잠겼나"를 `SCAN`으로 알아내야 하는데, `SCAN`의 `MATCH`는 서버측 필터일 뿐 커서는 DB 전체 키를 훑는다 — 회차 하나 조회에 (전체 키수 / COUNT)회 왕복이 들고, 이 Redis는 queue가 대기열 폴링을 처리하는 바로 그 인스턴스다.

같은 이유로 회차 목록의 판매수도 회차별 count 쿼리가 아니라 `group by` 한 번으로 받는다. 회차 목록은 입장 직후 첫 화면이라 정원 전체가 주기적으로 다시 부르는 경로다.

---

## 코드 구조

```
booking/src/main/java/com/cgv/booking/
├── BookingApplication.java   진입점(@EnableKafka)
├── web/                      REST 경계 (얇음 — 검증·위임만)
│   ├── MovieController        GET  /api/movies        (게이트 없음 — 입장 전 진입점)
│   ├── ScreeningController    GET  /api/screenings     회차 목록+잔여
│   ├── SeatController         GET  /api/seats · POST /select · /release
│   ├── BookingController      POST /api/bookings       결제·확정
│   └── ApiException(Handler)  도메인 오류 → HTTP 상태(400/403/404/409)
├── service/
│   ├── BookingService         confirm() 오케스트레이션(위 흐름)
│   ├── BookingPersistence     @Transactional 부분만 분리(프록시 통과 + timeout 5s)
│   ├── SeatRequest            좌석 요청 정규화(중복 제거·정렬·개수 상한)
│   ├── ScreeningService       회차+잔여(판매 group by + 락 인덱스 합산)
│   ├── SeatService            좌석도(3상태 합성) + 점유/해제
│   └── PaymentGateway         PG mock(approve/refund)
├── redis/
│   ├── SeatLockService        좌석락 Lua(LOCK·RELEASE·RENEW) + 회차별 인덱스 Set
│   └── AdmittedService        입장 인증(키+TTL) 발급·검사·삭제
├── kafka/
│   ├── AdmissionConsumer       admissions 소비 → 인증 발급 (후커밋)
│   ├── AdmissionExpiryConsumer admissions-revoked 소비 → 인증 삭제
│   ├── CompletedProducer       bookings-completed 발행
│   └── QueueEvent              {requestId, movieId, reason} 공용 형식
├── domain/                    JPA 엔티티(Movie·Screening·Seat·Booking·BookingSeat)
├── repo/                      Spring Data JPA 리포지토리
├── init/DataSeeder           도메인 시드(로컬 단일파드 전용, 기본 비활성)
├── config/
│   ├── CgvProps               application.yml의 cgv.* 픽스값 바인딩
│   └── KafkaConfig            admissions 소비 재시도 + DLT 격리
└── resources/
    ├── application.yml        설정 정본(datasource·kafka·actuator·tracing)
    └── logback-spring.xml     JSON 로그(MDC traceId/spanId)
```

**좌석락 Lua**는 원자성이 필요한 지점이다: `LOCK`(다중 좌석 all-or-nothing + 인덱스 기록), `RELEASE`(값이 나인 키만 DEL — 남의 락 안 건드림), `RENEW`(전부 내 락일 때만 TTL 연장 — 결제 도중 만료 방지).

---

## Kafka 계약

| 토픽 | 방향 | 값 | booking의 처리 |
|---|---|---|---|
| `admissions` | queue → booking | `{requestId, movieId}` | 인증 발급(SET+TTL, 멱등) |
| `admissions-revoked` | queue → booking | `{requestId, movieId, reason}` | 인증 삭제(DEL, 멱등) |
| `bookings-completed` | booking → queue | `{requestId, movieId}` | 확정 후 발행 |
| `admissions.DLT` | booking 내부 | 원본 메시지 | 재시도 소진분 격리 |

- **소비는 후커밋(at-least-once)**: 처리 → 커밋. 처리 전 죽어도 재전달되고, 발급·삭제 둘 다 멱등이라 중복이 무해하다.
- **재시도 창은 2초 × 5회 = 10초**다. 재시도는 리스너 스레드를 붙잡으므로 그 컨슈머가 맡은 파티션 전체가 그동안 멈춘다. 창을 짧게 잡아 Redis 파드 재기동(수 초)은 흡수하되 파티션 정지를 10초로 묶는다.
- **소진분은 DLT로 보낸다.** 메시지를 버리지 않고 격리해 파티션을 다시 흐르게 한다. DLT 토픽(`admissions.DLT`)이 브로커에 있어야 한다.
- **`reason`은 참고값이다.** 어떤 사유든 하는 일은 인증 삭제 하나라 처리 분기에 쓰지 않는다.
- **`QueueEvent`는 모르는 필드를 무시한다.** 두 서비스가 따로 배포되므로 한쪽만 먼저 올라간 구간이 항상 생긴다.

---

## 도메인 스키마

```
movies         (id, title, broadcast_at)                          1행  (생중계 = 영화 1, 18:00)
screenings     (id, movie_id, branch, screen_no, total_seats)     20행 (지점 5 × 관 4, 관당 200석)
seats          (id, screening_id, seat_row, seat_col, seat_no)    4,000행 (구조의 단일 진실원)
bookings       (id, screening_id, user_id, price, idempotency_key UNIQUE, created_at)
booking_seats  (id, booking_id, screening_id, seat_no, UNIQUE(screening_id, seat_no))
```

- `seats`에는 status 컬럼이 없다. "판매완료"는 `booking_seats`에 행이 있는지로 판정한다.
- 좌석을 `booking_seats`(예매당 여러 행)에 두는 이유는 `UNIQUE(screening, seat)` 제약을 걸려면 좌석이 행 단위여야 하기 때문이다.
- 단위: 대기·정원·입장 인증은 movie(방송) 단위, 좌석락만 screening(관) 단위다.

---

## 관측

**metric** — Actuator + Micrometer(Prometheus), `/actuator/prometheus`(내부 전용):

| 지표 | 답하는 것 |
|---|---|
| `hikaricp_connections_pending` | booking의 천장 신호(커넥션 풀 대기) |
| `booking.confirm{result}` | confirm() 종착점별 카운트 — `success`·`replay`·`lock_expired`·`seat_conflict`·`error`·`forbidden`·`no_screening`·`bad_request` |
| `booking.admissions{result}` | 인증 발급 소비 종착점(`ok`·`parse_error`·`missing_field`) |
| `booking.admission.revoked{result}` | 인증 회수 소비 종착점 |

프레임워크 지표가 구분하지 못하는 "새 예매 vs 멱등 replay", "진짜 락뚫림 vs 락만료"를 라벨로 구분한다.

**log**: logback JSON(logstash-encoder), STDOUT 한 곳. MDC의 `traceId`·`spanId`는 micrometer-tracing이 자동 주입한다.

**trace**: Micrometer Observation + `micrometer-tracing-bridge-otel` + OTLP(HTTP 4318). HTTP·Kafka span이 자동 생성·전파되어 queue와 W3C `traceparent`로 이어진다.

---

## 배포 관련 특징

- **memory limit이 필수**: 힙을 `MaxRAMPercentage=75`로 잡으므로 그 75%의 분모가 컨테이너 memory limit이다. limit이 없으면 JVM이 노드 전체를 분모로 착각한다.
- **startupProbe 필요**: JVM+Spring 초기화 + MySQL eager 연결로 기동이 수십 초 걸린다. liveness만 걸면 기동 중인 파드를 죽었다고 재시작한다.
- **readiness = MySQL·Redis만**: 둘 없으면 아무것도 못 한다. Kafka는 제외한다(공유 의존 + plain spring-kafka엔 health indicator가 없음).
- **graceful shutdown**: `server.shutdown: graceful`. `exec java`로 JVM이 PID 1로 SIGTERM을 직접 받는다.
- **DataSeeder는 배포 시 비활성**: `count()==0` 가드가 비원자라 멀티팟 동시 기동 시 PK 충돌이 난다. 로컬 단일파드만 `SEED_ON_START=true`로 켠다.

---

## 실행 / 엔드포인트

전체 스택은 루트 `docker compose up --build -d`로 띄운다(진입점 http://localhost:8088). 직노출 검증 포트는 `8091`.

| 메서드 | 경로 | 설명 |
|---|---|---|
| GET | `/api/movies` | 영화(방송) 목록 — 게이트 없음 |
| GET | `/api/screenings?movieId&requestId` | 회차 목록 + 잔여(게이트 O) |
| GET | `/api/seats?screeningId&requestId` | 좌석도 200칸(게이트 O) |
| POST | `/api/seats/select` · `/release` | 좌석 점유 / 해제 |
| POST | `/api/bookings` | 결제·확정 |
| GET | `/actuator/health/{liveness,readiness}` · `/actuator/prometheus` | 관측(내부 전용) |

로컬은 Redis standalone이다. `redis-sentinel` 프로파일을 켜면 Sentinel 경유로 접속한다.

---

## 환경변수

| env | 기본값 | 비고 |
|---|---|---|
| `PORT` | 8091 | |
| `MYSQL_HOST` / `PORT` / `DB` / `USER` / `PASSWORD` | localhost / 3306 / cgv / root / password | `PASSWORD`는 배포 시 Secret |
| `DB_POOL_SIZE` | 10 | HikariCP. 부하 실측으로 튜닝 |
| `REDIS_HOST` / `PORT` | localhost / 6379 | queue와 공유 |
| `REDIS_PASSWORD` | (없음) | 배포 시 Secret |
| `REDIS_MASTER_NAME` / `REDIS_SENTINEL_NODES` | (없음) | `redis-sentinel` 프로파일에서 사용 |
| `KAFKA_BROKER` | localhost:9092 | |
| `SEAT_LOCK_TTL` | 45s | 좌석락 TTL |
| `ADMITTED_TTL` | 180s | 입장 인증 만료 |
| `MAX_SEATS_PER_REQUEST` | 8 | 한 요청 좌석 수 상한 |
| `VIRTUAL_THREADS` | true | 로컬 진단 시 off 가능 |
| `OTLP_HTTP_ENDPOINT` | http://localhost:4318/v1/traces | Tempo/collector(HTTP) |
| `DDL_AUTO` / `SEED_ON_START` | none / false | 로컬 단일파드만 update / true |

세 시계의 순서: `SEAT_LOCK_TTL(45) < SESSION_TIMEOUT(60, queue) < ADMITTED_TTL(180)`.
좌석락은 세션보다 먼저 풀려야 자리를 잃은 사용자가 좌석을 붙잡지 않고, 인증은 세션보다 늦게 만료돼야 정상 세션이 중간에 끊기지 않는다.

---

## 알려진 한계

- **SIGKILL 갭**: graceful은 SIGTERM만 커버한다. "PG 승인 후 커밋 전 SIGKILL(OOM·전원단절)" 경로는 여전히 뚫린다.
- **완료발행 outbox 미도입**: `bookings-completed` 발행 실패는 로그로 관측하되 무손실 보장은 아니다 — queue의 세션 타임아웃이 최후 회수한다.
- **스키마 마이그레이션 도구 없음**: Flyway/Liquibase가 없어 `DDL_AUTO=none`으로 두면 스키마를 만들 주체가 없다. 로컬은 `update`+시드로 대체하고, 그 때문에 replica 증설이 막혀 있다.
- **테스트 없음**: `src/test`가 없고 이미지 빌드는 `-DskipTests`다. 동시 예매 차단·멱등 재시도 같은 불변식이 자동 검증으로 고정돼 있지 않다.
- **PG는 mock**: 실 PG 연동 시 실패코드·재시도·webhook·amount 검증이 추가된다.
- **JDBC 전송보안**: `useSSL=false&allowPublicKeyRetrieval=true`가 URL에 리터럴로 있어 인프라 레이어에서 끌 수 없다. 내부망 전제.
- **사이징 미측정**: 파드당 동시 세션·정원·MySQL 스펙은 부하테스트로 확정한다.
