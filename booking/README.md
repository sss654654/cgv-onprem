# booking — 예매·결제 서비스 (Java / Spring Boot)

queue가 정원 게이트로 통과시킨 사용자에 대해 결제를 처리하고 좌석 예매를 확정하는 서비스. queue와 처리 성격이 반대여서(요청 폭주 대신 트랜잭션 정합성) 저장소와 런타임도 다르게 구성한다.

- 역할: 좌석 선점 · 결제(PG mock) · 예매 확정 · 이중판매 차단
- 저장소: MySQL(확정 예매, 영속) + Redis(좌석락·입장 인증)
- 통신: 클라이언트 ↔ REST / queue ↔ Kafka 두 토픽
- 런타임: JVM 21, 가상 스레드

---

## 현재 상태

로컬 `docker-compose`에서 단일 프로세스로 동작한다. 로직·관측 배선은 완료됐고 Docker 빌드(Java 21)를 통과했다. k3s 배포는 다음 단계이며, 파드 스펙(메모리·풀 크기·정원)과 스키마 마이그레이션 Job은 아직 없다(로컬은 부팅 시 시드로 대체).

---

## 시스템에서의 위치

```
   브라우저
     │
     ▼
  frontend (게이트웨이 / Ingress)
     │  /api/movies · /api/screenings · /api/seats · /api/bookings
     ▼
 ┌──────────────┐  ◀── admissions (입장 승인) ─────── queue
 │   booking    │  ─── bookings-completed (자리 반환) ─▶
 │  (이 서비스) │
 └───┬──────┬───┘
     │      │
     ▼      ▼
  MySQL   Redis
 (확정)  (좌석락·admitted)
```

booking의 모든 동작은 `admitted` 게이트를 통과해야 한다. `admitted`는 booking이 직접 채우지 않고, queue가 승격할 때 보낸 Kafka `admissions`를 소비해 채운다. 예매가 끝나면 `bookings-completed`를 발행해 queue가 자리를 반환하게 한다(닫힌 순환).

---

## 요청 흐름 — `POST /api/bookings` 한 요청의 3국면

`BookingService.confirm()`이 오케스트레이션한다. 트랜잭션(DB)만 별도 빈으로 분리돼 있다.

```
 요청: { screeningId, seatNos, requestId, idempotencyKey }
   │
   ├─ ① 세 관문 ────────────────────────────────────────────
   │    게이트   : SISMEMBER admitted:{movie}   실패 → 403
   │    멱등     : idempotency_key 존재?         있으면 기존 결과 반환(재결제 안 함)
   │    락 재확인: renewMine(내 좌석 TTL 연장)   실패 → 409
   │
   ├─ ② PG 승인 (동기, 트랜잭션 밖) ─────────────────────────
   │    pg.approve()  →  돈 빠짐 (mock)
   │
   ├─ ③ 확정 (트랜잭션) ────────────────────────────────────
   │    @Transactional { INSERT bookings + booking_seats }
   │      UNIQUE(screening,seat) 위반 → 롤백 → safeRefund(환불)
   │      기타 커밋 실패(DB 단절·데드락) → 롤백 → safeRefund
   │
   └─ ④ 후처리 (커밋 후, best-effort) ──────────────────────
        락 해제 · Kafka completed 발행 · admitted 소진
        (실패해도 로그만 — 예매는 이미 커밋됨. TTL·세션 타임아웃이 최후 회수)
```

- **PG 호출을 트랜잭션 밖에 둔 이유**: 느린 외부 결제가 DB 커넥션을 붙잡지 않게 하기 위해서다. `confirm()`(트랜잭션 없음)이 `persist()`(트랜잭션 있음)를 별도 빈으로 호출해 프록시를 통과시킨다. 같은 클래스에서 `this.persist()`로 부르면 프록시를 안 거쳐 `@Transactional`이 무효가 된다.
- **환불 보상**: PG 승인 후 커밋에 실패하면 "돈은 빠졌는데 예매도 환불도 없는" 상태가 생긴다. UNIQUE 위반뿐 아니라 어떤 커밋 실패에서도 `safeRefund`로 돌려준다.

---

## 이중판매 방어 — 락은 필터, UNIQUE는 보증

동시에 같은 좌석을 두 명이 사는 것을 두 겹으로 막는다.

| 층 | 수단 | 성격 |
|---|---|---|
| 1차 필터 | Redis `SET NX`(좌석락) | 동시 요청을 Redis 단일 스레드가 직렬 처리해 대부분 조기 차단. TTL 만료·failover에 뚫릴 수 있음 |
| 최종 보증 | MySQL `UNIQUE(screening, seat)` | 락이 뚫려도 커밋 순간 원자로 두 번째를 거부(진 쪽은 롤백+환불). 뚫리지 않음 |

둘 다 저장소가 하나(Redis 하나·MySQL 하나)라 파드 수와 무관하게 직렬 판정된다. 트랜잭션은 동시성 방어가 아니라 정합(절반 반영 방지)을 담당하고, 동시성은 UNIQUE가 막는다.

---

## 좌석 상태 3개

화면은 "선택가능 / 선점됨 / 내선택"의 2색+1이지만, 서버는 3개를 구분한다.

```
   빈자리        ─ 어느 쪽에도 없음
   임시점유      ─ Redis  seat:{screeningId}:{seatNo}   (SET NX EX, TTL 45s)
   판매완료      ─ MySQL  booking_seats 행 존재          (영구)

   화면 응답 taken = (판매완료 OR 임시점유)
```

임시점유는 TTL로 사라져 다시 빈자리가 되지만 판매완료는 영구다. 그래서 잔여 좌석은 반드시 두 출처를 합산한다(`total − 판매완료 − 임시점유`). Redis만 세면 이미 팔린 좌석을 빈자리로 오표시한다.

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
│   └── ApiException(Handler)  도메인 오류 → HTTP 상태(403/404/409)
├── service/
│   ├── BookingService         confirm() 오케스트레이션(위 3국면)
│   ├── BookingPersistence     @Transactional 부분만 분리(프록시 통과용)
│   ├── ScreeningService       회차+잔여(MySQL 판매 + Redis 락 합산)
│   ├── SeatService            좌석도(3상태 합성) + 점유/해제
│   └── PaymentGateway         PG mock(approve/refund)
├── redis/
│   ├── SeatLockService        좌석락 Lua 3종(LOCK·RELEASE·RENEW) + SCAN
│   └── AdmittedService        입장 인증 Set(SISMEMBER 게이트)
├── kafka/
│   ├── AdmissionConsumer      admissions 소비 → SADD admitted (후커밋)
│   ├── CompletedProducer      bookings-completed 발행
│   └── QueueEvent             {requestId, movieId} 공용 형식
├── domain/                    JPA 엔티티(Movie·Screening·Seat·Booking·BookingSeat)
├── repo/                      Spring Data JPA 리포지토리
├── init/DataSeeder           도메인 시드(로컬 단일파드 전용, 기본 비활성)
├── config/
│   ├── CgvProps               application.yml의 cgv.* 픽스값 바인딩
│   └── KafkaConfig            admissions 소비 에러 핸들러(재시도 백오프 약 40s + 소진 시 ERROR)
└── resources/
    ├── application.yml        설정 정본(datasource·kafka·actuator·tracing)
    └── logback-spring.xml     JSON 로그(MDC traceId/spanId)
```

**좌석락 Lua 3종**은 원자성이 필요한 지점이다: `LOCK`(다중 좌석 all-or-nothing), `RELEASE`(값이 나인 키만 DEL — 남의 락 안 건드림), `RENEW`(전부 내 락일 때만 TTL 연장 — 결제 도중 만료 방지). 좌석 목록 조회는 `KEYS` 대신 `SCAN`(비블로킹)을 쓴다.

---

## 도메인 스키마

```
movies         (id, title, broadcast_at)                          1행  (생중계 = 영화 1, 18:00)
screenings     (id, movie_id, branch, screen_no, total_seats)     20행 (지점 5 × 관 4, 관당 200석)
seats          (id, screening_id, seat_row, seat_col, seat_no)    4,000행 (구조의 단일 진실원)
bookings       (id, screening_id, user_id, price, idempotency_key UNIQUE, created_at)
booking_seats  (id, booking_id, screening_id, seat_no, UNIQUE(screening_id, seat_no))
```

- `seats`에는 status 컬럼이 없다. "판매완료"는 `booking_seats`에 행이 있는지로 판정한다. 좌석 구조를 `seats`가 단독으로 소유하므로 화면 격자와 재고가 어긋날 수 없다.
- 좌석을 `booking_seats`(예매당 여러 행)에 두는 이유는 `UNIQUE(screening, seat)` 제약을 걸려면 좌석이 행 단위여야 하기 때문이다.
- 단위: 대기·정원·입장 인증은 movie(방송) 단위, 좌석락만 screening(관) 단위다. 사용자는 방송에 줄 서고, 입장 후 관을 고른다.

---

## 관측

**metric** — Actuator + Micrometer(Prometheus), `/actuator/prometheus`(내부 전용):
- 자동: HikariCP 풀(`hikaricp_connections_pending`이 booking의 천장 신호)·JVM·HTTP·Kafka.
- 손으로 심은 도메인 지표 하나: `booking.confirm{result=...}` — confirm() 종착점 7종(`success`·`replay`·`lock_expired`·`seat_conflict`·`error`·`forbidden`·`no_screening`). 프레임워크 지표가 구분하지 못하는 "새 예매 vs 멱등 replay", "진짜 락뚫림 vs 락만료"를 이 라벨로 구분한다.

**log**: logback JSON(logstash-encoder), STDOUT 한 곳. MDC의 `traceId`·`spanId`는 micrometer-tracing이 자동 주입한다.

**trace**: Micrometer Observation + `micrometer-tracing-bridge-otel` + OTLP(HTTP 4318). HTTP·Kafka span이 자동 생성·전파되므로 별도 코드 변경이 없다. queue와 W3C `traceparent`로 이어진다.

---

## 배포 관련 특징

booking의 결제 로직이 인프라 요구사항으로 이어진다.

- **memory limit이 필수**: 힙을 `MaxRAMPercentage=75`로 잡으므로 그 75%의 분모가 컨테이너 memory limit이다. limit이 없으면 JVM이 노드 전체를 분모로 착각해 OOMKilled로 즉시 종료된다.
- **startupProbe 필요**: JVM+Spring 초기화 + MySQL eager 연결로 기동이 수십 초 걸린다(액추에이터 `initialization-fail-timeout` 60초). liveness만 걸면 기동 중인 파드를 죽었다고 재시작한다.
- **readiness = MySQL·Redis만**: 둘 없으면 아무것도 못 한다. Kafka는 제외한다(공유 의존 + plain spring-kafka엔 health indicator가 없음).
- **graceful shutdown**: `server.shutdown: graceful`. 결제 진행 중 SIGTERM이 커밋 전에 도착해 발생하는 결제 불일치를 막는다. `exec java`로 JVM이 PID 1로 SIGTERM을 직접 받는다.
- **DataSeeder는 배포 시 비활성**: `count()==0` 가드가 비원자(TOCTOU)라 멀티팟 동시 기동 시 PK 충돌이 난다. 스키마·시드는 배포 Job이 단일 실행하고, 로컬 단일파드만 `SEED_ON_START=true`로 켠다.

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

---

## 환경변수

| env | 기본값 | 비고 |
|---|---|---|
| `PORT` | 8091 | |
| `MYSQL_HOST` / `PORT` / `DB` / `USER` / `PASSWORD` | localhost / 3306 / cgv / root / password | `PASSWORD`는 배포 시 Secret |
| `DB_POOL_SIZE` | 10 | HikariCP. 부하 실측으로 튜닝(X 측정) |
| `REDIS_HOST` / `PORT` | localhost / 6379 | queue와 공유 |
| `KAFKA_BROKER` | localhost:9092 | |
| `SEAT_LOCK_TTL` | 45s | 좌석락 TTL. `< SESSION_TIMEOUT` 불변식 |
| `VIRTUAL_THREADS` | true | 로컬 진단 시 off 가능 |
| `OTLP_HTTP_ENDPOINT` | http://localhost:4318/v1/traces | Tempo/collector(HTTP) |
| `DDL_AUTO` / `SEED_ON_START` | none / false | 로컬 단일파드만 update / true |

---

## 알려진 한계 / 다음

- **SIGKILL 갭**: graceful은 SIGTERM만 커버한다. "PG 승인 후 커밋 전 SIGKILL(OOM·전원단절)" 경로는 여전히 뚫린다. 완전 방어(intent 패턴 + 대사 배치)는 보류.
- **완료발행 outbox 미도입**: `bookings-completed` 발행 실패는 콜백에서 ERROR 로그로 관측하되(무손실 보장은 아님) "자리 반환 지연"으로 수용 — 세션 타임아웃이 최후 회수한다.
- **PG는 mock**: 실 PG 연동 시 실패코드·재시도·webhook·amount 검증이 추가된다.
- **사이징 미측정**: X(파드당 동시 세션)·정원·MySQL 스펙은 부하테스트로 확정한다. 병목 지표는 `hikaricp_connections_pending`.
- **JDBC 전송보안**: 현재 `useSSL=false`(내부망 전제). 배포 시 재검토.
