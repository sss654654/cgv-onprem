# queue-go — 폴링 기반 대기열 서비스 (Go)

티켓팅 대기열의 **입장 통제(admission control)** 서비스. 대기 인원을 대기열에 보관하고, booking으로는 booking이 감당할 정원만큼만 사용자를 들여보낸다. booking과는 직접 호출 없이 Kafka 이벤트로만 조율하므로 한쪽에 장애가 발생해도 다른 쪽으로 전파되지 않는다(장애 격리).

- 역할: 입장 정원 관리 · 대기 순번 계산 · 승격 · 타임아웃 회수
- 저장소: Redis (모든 상태). 자체 영속 저장소 없음 — 완전 stateless
- 통신: 클라이언트 ↔ HTTP 폴링 / booking ↔ Kafka 두 토픽

---

## 현재 상태

로컬 `docker-compose`에서 단일 프로세스로 동작한다. k3s 배포는 다음 단계이고 아직 클러스터에 올라가지 않았다. 배포에 필요한 코드 레벨 대비(관측·graceful·stateless)는 반영돼 있으나, 멀티팟 전용 기능(리더선출·rate 재설계·순번 캐싱)은 아직 없다. 클러스터 매니페스트는 별도 repo에서 관리한다.

---

## 시스템에서의 위치

```
   브라우저
     │
     ▼
  frontend (nginx 게이트웨이 · k3s에선 Traefik Ingress)
     │  /api/admission/*
     ▼
 ┌──────────────┐   admissions (입장 승인) ──────────▶ ┌──────────┐
 │   queue-go   │                                       │ booking  │
 │  (이 서비스) │ ◀────── bookings-completed (자리 반환) │          │
 └──────┬───────┘                                       └──────────┘
        │ 모든 상태
        ▼
      Redis
```

queue는 "누가 입장할 수 있나"(정원·명단·타임아웃)를 소유하고, booking은 그 안에 들어온 사용자의 행동(좌석·결제)을 담당한다. 두 서비스를 잇는 신호가 Kafka 두 토픽이다.

---

## 요청 흐름

전달 방식은 폴링이다. 서버는 push하지 않고, 클라이언트가 주기적으로 순번과 입장 여부를 조회한다.

```
클라이언트                        queue-go                      Redis
   │
   │ ① POST /enter              enter Lua
   ├───────────────────────▶   (정원 확인·원자)  ──ZADD──▶  active 또는 waiting
   │ ◀── 200 ADMITTED  /  202 WAITING(순번)
   │
   │ ② GET /position (1~5초 반복)  3-state 판정
   ├───────────────────────▶   ZSCORE→ZRANK  ─────────▶  순번 계산 + lastseen 도장
   │ ◀── { status, position, behind, etaSeconds }
   │
   │ ③ status = ADMITTED 발견 → 폴링 중단, 좌석 화면으로

백그라운드 루프 (사용자 요청과 무관하게 상시)
   승격      2s :  waiting 앞에서 빈자리만큼 active로  + Kafka admissions 발행
   active    10s:  입장시각이 낡은 세션 회수
   waiting   10s:  폴링이 끊긴(lastseen 낡은) 대기자 회수
   완료 소비 상시:  booking의 bookings-completed 수신 → active 반환(ZREM)
```

- **enter** (`POST /api/admission/enter`): 정원 여유면 `ADMITTED`(200), 꽉 차면 `WAITING`(202)+순번. 즉시 입장(ADMITTED)일 때만 Kafka `admissions`를 발행해 booking이 좌석선택을 허용하게 한다.
- **position** (`GET /api/admission/position`): 3-state 판정. `ZSCORE active`(있으면 ADMITTED) → `ZRANK waiting`(있으면 WAITING+순번) → 둘 다 없으면 EXPIRED. active를 먼저 보는 이유는 승격된 사용자가 waiting에서 빠지므로, waiting부터 보면 승격자를 EXPIRED로 오판하기 때문이다. 폴링은 `waiting_lastseen`을 갱신하는 생존 신호도 겸한다.
- **leave / complete**: 자발적 이탈 / active 종료(자리 반환).

---

## 코드 구조

```
queue-go/
├── main.go            부팅·배선·graceful shutdown 오케스트레이션
├── logging.go         slog JSON 핸들러 + trace_id 주입(로그↔트레이스 상관)
├── tracing.go         OTel TracerProvider(OTLP/gRPC) + W3C propagator
├── config/            env 로드(없으면 로컬 기본값)
├── handler/
│   ├── admission.go   enter·position·leave·complete HTTP 핸들러
│   └── health.go      /health/live · /health/ready(Redis PING)
├── redis/             상태 저장 계층 — Lua 스크립트 + go-redis 래퍼
│   ├── client.go      래퍼(풀·타임아웃)·PoolStats 노출
│   ├── enter.go       enterScript(정원 확인 원자)
│   ├── promote.go     promoteScript + requeueFrontScript(발행 실패 롤백)
│   ├── timeout.go     expireScript / waitingExpireScript
│   ├── position.go    3-state 순번 조회(Lua 아님 — 읽기 위주)
│   ├── leave.go       Leave / CompleteActive(ZREM 멱등)
│   ├── query.go       WaitingCount·ActiveCount·ActiveQueueMovies
│   └── keys.go        키 빌더
├── processor/         백그라운드 루프
│   ├── queue.go       승격 루프(2s) + 발행 실패 롤백
│   ├── timeout.go     active 세션 타임아웃(10s)
│   └── waiting_timeout.go  폴링 끊긴 대기자 회수(10s)
├── kafka/kafka.go     admissions 발행 · bookings-completed 소비 · 실패 카운터
└── metrics/
    ├── prom.go        계측 정의 + /metrics 전용 서버 + 샘플러
    └── rate.go        승격 처리율(EMA) → ETA 재료
```

원자성이 필요한 연산(정원 확인·승격·만료·롤백)은 Lua로 묶고, 읽기 위주인 position은 개별 명령으로 조립한다. `redis/` 패키지가 go-redis 의존을 가둬 핸들러·프로세서는 래퍼 타입만 참조한다.

---

## Redis 상태

| 키 | 타입 | score / 값 | 용도 |
|---|---|---|---|
| `sessions:{movie}:active` | ZSet | 입장 시각(ms) | 정원 안 인원. score로 타임아웃 판정 |
| `sessions:{movie}:waiting` | ZSet | 진입 순서(ms) | 대기 줄. 순번(ZRANK)·선착순 |
| `waiting_lastseen:{movie}` | ZSet | 마지막 폴링(ms) | 생존 추적. 이탈 감지 근거 |
| `promoted_count:{movie}` | counter | INCRBY | 누적 승격 수 → rate·ETA |
| `active_movies` / `waiting_movies` | Set | — | 루프가 돌 대상 영화 추적 |

- `waiting`과 `waiting_lastseen`은 score 용도가 달라 분리한다(진입 순서 vs 마지막 폴링). 합치면 폴링할 때마다 순번이 밀린다. 멤버는 항상 함께 움직인다 — enter에서 둘 다 ZADD, 빼는 모든 경로에서 둘 다 ZREM(승격·만료는 Lua, leave는 트랜잭션 파이프라인으로 원자 처리).
- **이탈 감지는 last-seen 방식이다.** 폴링은 붙잡는 연결이 없어 "끊김"을 직접 못 잡는다. 대신 마지막 폴링 시각을 저장해두고, 그 시각이 cutoff(now − timeout)보다 오래되면 회수한다. 폴링을 계속하는 사용자는 시각이 갱신돼 걸리지 않고, 나간 사용자는 시각이 멈춰 회수 대상이 된다.

---

## 관측

세 축 모두 코드에 배선돼 있다(수집·저장은 클러스터 몫이라 로컬에선 흐르지 않는다).

**metric** — 별도 포트 `/metrics`(기본 9091), 앱 포트와 분리:

| 지표 | 답하는 것 |
|---|---|
| `queue_http_request_duration_seconds{path,status}` | RPS·p99·에러율(전부 이 히스토그램에서 파생) |
| `queue_waiting{movie}` · `queue_active{movie}` | 줄 길이 · 정원 소진율 |
| `queue_promotion_rate{movie}` | 초당 승격 수(사용자 ETA와 같은 소스) |
| `queue_loop_last_tick_timestamp_seconds{loop}` | 루프 심박 — `now − 값`이 커지면 stall |
| `queue_kafka_publish_failures_total` | 발행 실패 누적 |
| `queue_kafka_consume_failures_total` | 자리 반환 처리 실패 누적 |
| `queue_redis_pool{state}` | 풀 사용중·대기 |

CPU·스로틀·메모리는 cAdvisor가 제공하므로 심지 않는다 — 코드 밖에서 관측할 수 없는 지표만 심는다.

**log**: slog JSON. `slog.SetDefault`로 표준 `log` 출력까지 재라우팅돼 기존 호출부는 무변경. ctx에 span이 있으면 `trace_id`·`span_id`를 자동 첨부한다.

**trace**: OTel. otelgin 서버 span + Kafka 헤더의 W3C `traceparent` inject/extract로 queue→Kafka→booking이 하나의 trace가 된다. 전송처는 `OTLP_GRPC_ENDPOINT`(gRPC 4317).

---

## 배포 대비 특징

롤링·스케일·노드 분산으로 파드가 수시로 교체되는 환경을 코드가 전제한다.

- **완전 stateless** — 상태 100% Redis. 어느 파드가 받아도 결과가 같고, 늘리면 즉시 분산되며, 줄여도 폴링이라 끊길 연결이 없다.
- **graceful shutdown 4단계**: readiness 내림 → HTTP drain → 루프·consumer 정지 → 자원 close(의존 역순). SIGTERM에 처리 중 요청을 자르지 않는다.
- **probe**: liveness는 200 고정, readiness는 Redis PING만. Kafka는 제외한다 — 공유 의존이라 넣으면 전 파드가 동시에 503이 된다.
- **automaxprocs**: GOMAXPROCS를 cgroup CPU limit에 맞춘다.
- **기동 비동기화**: Kafka 토픽 보장을 백그라운드로 빼 Kafka가 늦어도 HTTP 포트는 즉시 열린다.
- **이미지**: multi-stage → distroless static `:nonroot`(uid 65532).

---

## Kafka 전달 보장

- **발행(admissions)**: `RequiredAcks=RequireOne` + 재시도 3회. 소진 후에도 실패하면 발행 에러를 버리지 않는다 — enter는 active를 보상 롤백하고 `RETRY_LATER`(503), 승격 루프는 발행 못 한 승격자를 waiting 선두로 원자 복귀(`RequeueFront`)하고 10초 승격을 멈춘다. 대기 순서는 유지된다.
- **소비(completed)**: `FetchMessage → 처리 → CommitMessages`(at-least-once). 처리 전 죽어도 재전달되고, `CompleteActive`가 ZREM 한 줄이라 중복 재처리가 무해하다. 파싱 불가(포이즌) 메시지는 커밋하고 스킵한다.
- **ensureTopic**: 컨슈머가 붙기 전 토픽을 멱등 생성한다 — 없는 토픽에 그룹이 먼저 붙으면 wedge되는 문제를 막고, 브로커 기동 순서 의존성도 흡수한다.

---

## 실행

```bash
cd cgv-onprem
docker compose up --build -d
```

- 브라우저 진입점: http://localhost:8088 (frontend 게이트웨이)
- 직노출(검증용): queue = 호스트 `18090`, `/metrics` = `19091`
- E2E: `./e2e2.sh` (게이트웨이 통해 enter→좌석→예매→승격 전 흐름 확인)

`MAX_SESSIONS=2`이므로 처음 2명은 즉시 입장, 이후는 대기(순번 폴링). 입장자가 예매를 완료하면 Kafka completed → queue가 자리를 반환 → 대기자가 다음 폴링에서 ADMITTED를 발견한다.

---

## 환경변수

| env | 기본값 | 비고 |
|---|---|---|
| `PORT` | 8090 | HTTP(폴링·enter·헬스) |
| `METRICS_PORT` | 9091 | /metrics 전용 |
| `REDIS_HOST` / `REDIS_PORT` | localhost / 6379 | |
| `REDIS_PASSWORD` | (없음) | 배포 시 Secret |
| `REDIS_POOL_SIZE` | 0(라이브러리 기본) | 부하 실측으로 확정 |
| `MAX_SESSIONS` | 2 | 데모 소정원. 실운영 = 측정값 |
| `SESSION_TIMEOUT` | 60s | active 수명(좌석락 45s < 세션 60s). 실운영 600 |
| `WAITING_TIMEOUT` | 30s | 폴링 주기의 넉넉한 배수 |
| `QUEUE_PROCESS_INTERVAL` | 2000ms | 승격 주기 |
| `PROCESSING_BATCH_SIZE` | 100 | 한 틱 승격 상한 |
| `SESSION_CLEANUP_INTERVAL` | 10000ms | active 만료 검사 주기 |
| `WAITING_CLEANUP_INTERVAL` | 10000ms | waiting 만료 검사 주기 |
| `KAFKA_BROKER` | localhost:9092 | |
| `OTLP_GRPC_ENDPOINT` | localhost:4317 | Tempo/collector(gRPC) |
| `GIN_MODE` | release | 텍스트 배너의 JSON 로그 오염 방지(config 밖 직접 read) |

---

## 알려진 한계 / 다음

- **멀티팟 미대응**: 로컬 단일 인스턴스 전제. 클러스터에서 붙일 것 = 리더선출(루프 중복 제거), rate/ETA 재설계(파드별 EMA → 공유 카운터 샘플링), 순번 캐싱(Redis CPU 포화 시 조건부).
- **런타임 관측 실측 대기**: trace가 Tempo에 닿는지, 로그에 trace_id가 붙는지, SIGTERM graceful drain은 관측 스택이 떠야 확인한다.
- **사이징 미측정**: Pod당 처리 RPS · PoolSize · HPA 임계는 부하테스트로 확정한다.
