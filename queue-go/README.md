# queue-go — 폴링 기반 대기열 서비스 (Go)

티켓팅 대기열의 **입장 통제(admission control)** 서비스. 대기 인원을 대기열에 보관하고, booking이 감당할 정원만큼만 사용자를 들여보낸다. booking과는 직접 호출 없이 Kafka 이벤트로만 조율하므로 한쪽 장애가 다른 쪽으로 전파되지 않는다.

- 역할: 입장 정원 관리 · 대기 순번 계산 · 승격 · 타임아웃 회수 · 입장/회수 통보
- 저장소: Redis (모든 상태). 자체 영속 저장소 없음 — 완전 stateless
- 통신: 클라이언트 ↔ HTTP 폴링 / booking ↔ Kafka 세 토픽

---

## 시스템에서의 위치

```
   브라우저
     │
     ▼
  frontend (정적 SPA · 클러스터에선 Traefik Ingress가 경로 분기)
     │  /api/admission/*
     ▼
 ┌──────────────┐   admissions          (입장 승인) ──────▶ ┌──────────┐
 │   queue-go   │   admissions-revoked  (입장 회수) ──────▶ │ booking  │
 │  (이 서비스) │ ◀────────── bookings-completed (자리 반환) │          │
 └──────┬───────┘                                            └──────────┘
        │ 모든 상태
        ▼
      Redis
```

queue는 "누가 입장할 수 있나"(정원·명단·타임아웃)를 소유하고, booking은 그 안에 들어온 사용자의 행동(좌석·결제)을 담당한다. 두 서비스를 잇는 신호가 Kafka 세 토픽이다.

**입장 인증의 수명은 queue가 정한다.** booking은 자기 인증을 자발적으로 만들지 않고, `admissions`로 받아 발급하고 `admissions-revoked`로 지운다. 그래서 정원 통제가 한 곳(queue)에만 있다.

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
   승격      2s :  waiting 앞에서 빈자리만큼 active로 + 저널 기록 → admissions 발행
   active   10s :  입장시각이 낡은 세션 회수 + 저널에 회수 기록
   waiting  10s :  폴링이 끊긴(lastseen 낡은) 대기자 회수
   저널 스윕 5s :  발행이 끝나지 못한 저널 항목을 재발행
   완료 소비 상시:  booking의 bookings-completed 수신 → active 반환(ZREM)
```

- **enter** (`POST /api/admission/enter`) — 정원 여유면 `ADMITTED`(200), 꽉 차면 `WAITING`(202)+순번. `ADMITTED`일 때만 `admissions`를 발행한다.
- **position** (`GET /api/admission/position`) — 3-state 판정. `ZSCORE active`(있으면 ADMITTED) → `ZRANK waiting`(있으면 WAITING+순번) → 둘 다 없으면 EXPIRED. active를 먼저 보는 이유는 승격된 사용자가 waiting에서 빠지기 때문이다. 폴링은 `waiting_lastseen`을 갱신하는 생존 신호도 겸한다.
- **stats** (`GET /api/admission/stats`) — 전체 현황(입장·대기·누적 승격·정원·신규 진입자 예상 대기). 파이프라인 한 왕복 집계, 읽기 전용. 프론트의 현황 타일이 3초마다 부른다.
- **events** (`GET /api/admission/events?after=`) — 실황 피드. `after`(마지막으로 본 id) 이후만 돌려준다. 프론트가 3초마다 부르고, 새 이벤트가 없으면 0건이다.
- **leave** (`POST /api/admission/leave`) — 자발적 이탈. active·waiting·lastseen에서 전부 제거.
- **complete** (`POST /api/admission/complete`) — 사용자가 부른 자리 반환. active에서만 제거.
- **reset** (`POST /api/admission/reset`) — 한 영화의 대기열 상태 전부 삭제(운영용). `X-Admin-Token` 헤더로 검증하며, **`ADMIN_TOKEN`을 주입하지 않은 배포에는 라우트 자체가 등록되지 않는다** — 403을 돌려주는 방식이면 API의 존재가 드러나서다. 예매 기록·좌석 쪽 초기화는 booking의 `/api/admin/reset`이 맡는다.

`leave`·`complete`는 그 사용자가 active였으면 **회수 이벤트를 저널에 남긴다.** 남기지 않으면 booking의 입장 인증이 그대로 남아 정원 밖 사용자가 예매 API를 계속 통과한다.

---

## 발행 대기 저널

상태 변경(승격·입장·타임아웃·이탈)은 Redis Lua 안에서 원자로 끝나지만, Kafka 발행은 그 뒤의 별개 동작이다. 둘 사이에서 파드가 죽거나 발행이 실패하면 두 방향으로 어긋난다.

- 입장 통보가 안 나가면 → 사용자는 화면상 입장했는데 booking은 몰라 좌석선택이 403
- 회수 통보가 안 나가면 → 자리를 잃은 사용자가 booking 인증에 남아 정원 밖에서 예매 가능

그래서 상태 변경과 **같은 Lua 실행 안에서** 발행 대기 저널(`pending_events` ZSet)에 기록하고, 발행에 성공해야만 지운다. 지워지지 않고 남은 것은 스윕 루프가 다시 내보낸다.

```
member = "<kind>|<reason>|<requestId>"      score = 상태변경 시각(ms)

kind    A = 입장 통보(admissions)   R = 입장 회수(admissions-revoked)
reason  ENTER · PROMOTE · SESSION_TIMEOUT · LEAVE · COMPLETE
```

- 정상 경로도 저널에 쓰고 지운다. 방금 쓴 항목까지 스윕이 집으면 중복 발행이 되므로 **유예(grace 6초)**를 지난 항목만 대상으로 한다.
- 그래도 중복은 날 수 있다. 소비 측 처리가 멱등(발급=SET, 회수=DEL)이라 **유실보다 중복을 택한다.**
- 회수를 기록할 때는 아직 발행 못 한 입장 통보(`A|ENTER|` · `A|PROMOTE|`)를 같은 실행에서 지운다. 안 지우면 스윕이 뒤늦게 "입장했다"를 발행해 방금 회수한 인증이 되살아난다.

**되돌리기(롤백)는 하지 않는다.** 발행 실패 시 승격자를 대기열로 되돌리는 방식은 "발행 실패 = 브로커에 안 들어감"을 전제하는데, ctx 취소·타임아웃은 브로커 append 이후에도 에러로 돌아온다. 되돌리면 booking에는 입장이 들어갔는데 queue만 자리를 뺏는 상태가 된다. 저널에 남겨 재발행하는 쪽이 그 경로를 만들지 않는다.

발행이 실패하면 승격을 10초 멈춘다. 중단 플래그는 파드 로컬 변수가 아니라 **Redis 키(TTL)**라, replica가 여럿이어도 전 파드가 함께 쉰다.

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
│   ├── enter.go       enterScript(정원 확인 + 추적 등록 + 저널 기록, 원자)
│   ├── promote.go     promoteScript(승격 + 저널 기록, 원자)
│   ├── timeout.go     expireScript / waitingExpireScript(만료 + 회수 저널)
│   ├── leave.go       leaveScript / releaseScript / CompleteActive
│   ├── untrack.go     빈 대기열의 추적 Set 해제(원자 — 비었을 때만)
│   ├── pending.go     저널 조회·정리, 승격 중단 플래그
│   ├── position.go    positionScript(3-state 판정 + 생존 도장, 원자·왕복 1회)
│   ├── events.go      실황 피드 — 이벤트 기록(ZAdd)·읽기(after 이후만)
│   ├── rate.go        승격 처리율 — 초 단위 버킷 기록·창(90초) 평균 조회
│   ├── stats.go       현황 집계(active·waiting·promoted, 파이프라인 1왕복)
│   ├── reset.go       대기열 초기화(운영용 — 키 계산으로 일괄 삭제, SCAN 없음)
│   ├── query.go       WaitingCount·ActiveCount·ActiveQueueMovies
│   └── keys.go        키 빌더
├── processor/         백그라운드 루프
│   ├── queue.go       승격(2s) — 승격 후 배치 발행, 실패 시 저널 보존
│   ├── sweep.go       저널 스윕(5s) — 발행이 끝나지 못한 항목 재발행
│   ├── timeout.go     active 세션 타임아웃(10s)
│   └── waiting_timeout.go  폴링 끊긴 대기자 회수(10s)
├── kafka/kafka.go     admissions·admissions-revoked 발행 · bookings-completed 소비
└── metrics/
    ├── prom.go        계측 정의 + /metrics 전용 서버 + 샘플러
    └── rate.go        RateProvider — Redis 공유 버킷의 창 평균을 짧게 캐시해 ETA·지표에 공급
```

원자성이 필요한 연산(정원 확인·승격·만료·이탈)은 Lua로 묶는다. **읽기인 position도 Lua다** — ZSCORE와 ZRANK를 따로 왕복하면 그 사이에 승격이 끼어, waiting에서 빠진 사람을 "둘 다 없음"으로 읽고 방금 입장한 사용자에게 EXPIRED를 돌려준다. 판정 순서만으로는 그 창이 안 닫힌다. `redis/` 패키지가 go-redis 의존을 가둬 핸들러·프로세서는 래퍼 타입만 참조한다.

---

## Redis 상태

| 키 | 타입 | score / 값 | 용도 |
|---|---|---|---|
| `sessions:{movie}:active` | ZSet | 입장 시각(ms) | 정원 안 인원. score로 타임아웃 판정 |
| `sessions:{movie}:waiting` | ZSet | 진입 순서(ms) | 대기 줄. 순번(ZRANK)·선착순 |
| `waiting_lastseen:{movie}` | ZSet | 마지막 폴링(ms) | 생존 추적. 이탈 감지 근거 |
| `promoted_count:{movie}` | counter | INCRBY | 누적 승격 수(현황 표시용) |
| `promote_rate:{movie}:{unix초}` | counter(TTL) | INCRBY | 초 단위 승격 버킷. 창(90초) 합산 평균 = 처리율 → ETA. TTL로 자멸 |
| `pending_events:{movie}` | ZSet | 상태변경 시각(ms) | 발행 대기 저널 |
| `queue:promote_pause` | string(TTL) | "1" | 발행 실패 시 전 파드 공통 승격 중단 |
| `active_movies` / `waiting_movies` | Set | — | 루프가 돌 대상 영화 추적 |
| `events:{movie}` | ZSet(TTL) | 이벤트 id | 화면용 실황 피드. 최근 100건만 유지 |
| `events_id:{movie}` | counter(TTL) | INCRBY | 피드 id 발급. 클라가 "어디까지 봤나"를 이 값으로 가른다 |

- `waiting`과 `waiting_lastseen`은 score 용도가 달라 분리한다(진입 순서 vs 마지막 폴링). 합치면 폴링할 때마다 순번이 밀린다. 멤버는 항상 함께 움직인다 — enter에서 둘 다 ZADD, 빼는 모든 경로에서 둘 다 ZREM.
- **이탈 감지는 last-seen 방식이다.** 폴링은 붙잡는 연결이 없어 "끊김"을 직접 못 잡는다. 마지막 폴링 시각을 저장해두고 cutoff(now − timeout)보다 오래되면 회수한다.
- **실황 피드가 List가 아니라 ZSet인 이유는 읽는 쪽 조건에 있다.** score를 이벤트 id로 두면 "id가 after보다 큰 것"을 Redis가 잘라서 보낸다 — 새 이벤트가 없으면 0건이 온다. List면 그 조건을 서버에서 못 걸어 보관분 전체(100건, 약 6KB)를 받아 앱에서 잘라야 하고, 피드는 화면마다 3초로 폴링하는 경로라 그 전송이 대부분 낭비가 된다.

---

## Kafka 계약

| 토픽 | 방향 | 값 | 키 |
|---|---|---|---|
| `admissions` | queue → booking | `{requestId, movieId}` | `requestId` |
| `admissions-revoked` | queue → booking | `{requestId, movieId, reason}` | `requestId` |
| `bookings-completed` | booking → queue | `{requestId, movieId}` | — |

- **파티션 키 = `requestId`.** 같은 사용자의 입장·회수가 같은 파티션에서 순서대로 처리된다. 키가 없으면 두 이벤트가 다른 파티션에 흩어져 "회수 → 입장" 순으로 처리될 수 있고, 그러면 이미 지운 인증이 되살아난다. 사용자 간에는 순서 의존이 없어 `requestId` 단위 분산이 파티션을 고르게 쓰면서 필요한 순서만 지킨다.
- **발행은 배치다.** 승격 배치와 스윕은 여러 이벤트를 한 번의 쓰기로 보낸다. 건별로 보내면 메시지마다 Writer 배치 타이머(기본 1초)를 기다려 배치가 통째로 느려진다. `BatchTimeout`은 10ms로 낮춰 부분 배치도 오래 붙들지 않는다.
- **발행 ack**: `RequiredAcks=RequireOne` + 재시도 3회. 소진 후 실패면 에러를 돌려주고, 호출자는 저널을 남긴다.
- **소비(completed)**: `FetchMessage → 처리 → CommitMessages`(at-least-once). 처리 전 죽어도 재전달되고, `CompleteActive`가 ZREM이라 중복 재처리가 무해하다. 파싱 불가 메시지는 커밋하고 스킵한다.
- **`reason`은 회수 이벤트에만 실린다.** 관측·디버깅용이고 소비 분기에는 쓰이지 않는다 — 어떤 사유든 booking이 하는 일은 인증 삭제 하나다.

---

## 관측

**metric** — 별도 포트 `/metrics`(기본 9091), 앱 포트와 분리:

| 지표 | 답하는 것 |
|---|---|
| `queue_http_request_duration_seconds{path,status}` | RPS·p99·에러율 |
| `queue_waiting{movie}` · `queue_active{movie}` | 줄 길이 · 정원 소진율 |
| `queue_promotion_rate{movie}` | 초당 승격 수(사용자 ETA와 같은 소스) |
| `queue_loop_last_tick_timestamp_seconds{loop}` | 루프 심박 — `now − 값`이 커지면 stall |
| `queue_kafka_publish_failures_total` | 발행 실패 누적 |
| `queue_kafka_consume_failures_total` | 자리 반환 처리 실패 누적 |
| `queue_redis_pool{state}` | 풀 사용중·대기 |

루프 심박은 `promote` · `session_timeout` · `waiting_timeout` · `pending_sweep` 넷을 찍는다.
CPU·스로틀·메모리는 cAdvisor가 제공하므로 심지 않는다 — 코드 밖에서 관측할 수 없는 지표만 심는다.

**log**: slog JSON. `slog.SetDefault`로 표준 `log` 출력까지 재라우팅된다. ctx에 span이 있으면 `trace_id`·`span_id`를 자동 첨부한다.

**trace**: OTel. otelgin 서버 span + Kafka 헤더의 W3C `traceparent` inject/extract로 queue→Kafka→booking이 하나의 trace가 된다. 전송처는 `OTLP_GRPC_ENDPOINT`(gRPC 4317).

---

## 배포 대비 특징

- **완전 stateless** — 상태 100% Redis. 어느 파드가 받아도 결과가 같다.
- **graceful shutdown 4단계**: readiness 내림 → HTTP drain → 루프·consumer 정지 → 자원 close(의존 역순).
- **probe**: liveness는 200 고정, readiness는 Redis PING만. Kafka는 제외한다 — 공유 의존이라 넣으면 전 파드가 동시에 503이 된다.
- **automaxprocs**: GOMAXPROCS를 cgroup CPU limit에 맞춘다.
- **기동 비동기화**: Kafka 토픽 보장을 백그라운드로 빼 Kafka가 늦어도 HTTP 포트는 즉시 열린다.
- **이미지**: multi-stage → distroless static `:nonroot`(uid 65532).
- **enter의 발행은 요청 ctx와 분리한다.** 클라이언트가 응답을 기다리다 끊으면 요청 ctx가 취소되는데, 그 취소가 발행 실패로 오판되면 되돌리기 경로가 열린다. 별도 타임아웃(6초) ctx로 보낸다.

---

## 실행

```bash
cd cgv-onprem
docker compose up --build -d
```

- 브라우저 진입점: http://localhost:8088 (frontend)
- 직노출(검증용): queue = 호스트 `18090`, `/metrics` = `19091`
- E2E: `./e2e2.sh`

`MAX_SESSIONS=2`이므로 처음 2명은 즉시 입장, 이후는 대기. 입장자가 예매를 완료하면 Kafka completed → queue가 자리를 반환 → 대기자가 다음 폴링에서 ADMITTED를 발견한다.

로컬은 Redis standalone이다. `REDIS_SENTINEL_ADDRS`가 없으면 단일 주소로 접속하고, 있으면 Sentinel 경유(`NewFailoverClient`)로 붙는다.

---

## 환경변수

| env | 기본값 | 비고 |
|---|---|---|
| `PORT` | 8090 | HTTP(폴링·enter·헬스) |
| `METRICS_PORT` | 9091 | /metrics 전용 |
| `REDIS_HOST` / `REDIS_PORT` | localhost / 6379 | standalone 경로 |
| `REDIS_MASTER_NAME` | (없음) | Sentinel 마스터셋 이름 |
| `REDIS_SENTINEL_ADDRS` | (없음) | 있으면 Sentinel 경유로 접속 |
| `REDIS_PASSWORD` | (없음) | 배포 시 Secret |
| `REDIS_POOL_SIZE` | 0(라이브러리 기본) | 부하 실측으로 확정 |
| `MAX_SESSIONS` | 2 | 데모 소정원. 실운영 = 측정값 |
| `SESSION_TIMEOUT` | 60s | active 수명(좌석락 45s < 세션 60s). 실운영 600 |
| `WAITING_TIMEOUT` | 30s | 폴링 주기의 넉넉한 배수 |
| `QUEUE_PROCESS_INTERVAL` | 2000ms | 승격 주기 |
| `PROCESSING_BATCH_SIZE` | 100 | 한 틱 승격 상한 |
| `SESSION_CLEANUP_INTERVAL` | 10000ms | active 만료 검사 주기 |
| `WAITING_CLEANUP_INTERVAL` | 10000ms | waiting 만료 검사 주기 |
| `PENDING_SWEEP_INTERVAL` | 5000ms | 저널 스윕 주기 = 회수가 booking에 닿는 최대 지연 |
| `KAFKA_BROKER` | localhost:9092 | |
| `OTLP_GRPC_ENDPOINT` | localhost:4317 | Tempo/collector(gRPC) |
| `ADMIN_TOKEN` | (없음) | 초기화 API 인증. 비어 있으면 그 라우트가 등록되지 않는다 |
| `GIN_MODE` | release | 텍스트 배너의 JSON 로그 오염 방지 |

`SESSION_TIMEOUT`을 바꾸면 booking의 `ADMITTED_TTL`도 같이 올려야 한다. booking의 인증 TTL이 queue의 세션 수명보다 짧으면 정상 세션이 중간에 인증을 잃는다.

---

## 알려진 한계

- **루프 중복 수행**: 백그라운드 루프는 모든 파드가 함께 돈다 — 승격 Lua가 정원을 원자로 확인하므로 초과 승격은 안 나지만, 같은 검사를 여러 파드가 반복한다. (ETA는 값이 Redis 공유 버킷이라 파드가 여럿이어도 같은 값을 읽는다.)
- **movieId 실재 검증 없음**: 형식 검증(영숫자·`_`·`-`, 최대 64자)은 핸들러에 있지만 영화가 실제 존재하는지는 확인하지 않는다 — 형식에 맞는 임의 movieId로도 대기열 키·추적 등록이 생긴다. 폴링이 끊기면 30초 evict 후 유휴 추적 해제로 자가 정리되지만, 게이지 라벨은 남는다.
- **테스트 없음**: 자동 테스트가 없다. 검증은 빌드·vet·로컬 E2E까지다.
- **사이징 미측정**: 파드당 처리 RPS · PoolSize · HPA 임계는 부하테스트로 확정한다.
