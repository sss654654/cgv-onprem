# queue-go — 대기열 서비스 (Go) 구조 설명

> **2026-07-02 폴링 전환 반영** — SSE(stream·Hub·Pub/Sub 방송) 전부 제거, `GET /position` 폴링으로 대체.

> 옛 Java `cgv-queue-system`의 대기열 코어를 Go로 재구축한 것. 설계 진실 = `로드맵/단계2-dev-k3s구축배포/2-1-CGV-구축Go전환/백엔드서비스-올인원.md` §1 (SSE로 판 분석은 `대기열-SSE흐름-레거시.md`에 보존).
> **하는 일:** 수만 동시 접속에서 "정원만큼만 입장(active)시키고 나머진 선착순 대기(waiting) → 자리 나면 자동 승격"을 Redis로 처리한다. **서버는 push하지 않는다 — 클라가 주기적으로(1-5초 적응형) "내 순번? 입장됐나?"를 pull한다.**
> **스택:** Go 1.26 + Gin(HTTP) + go-redis(Redis·Lua) + segmentio/kafka-go(Kafka). 상태(state)는 전부 Redis에 있고 앱은 무상태(stateless) — 연속 폴링이 매번 다른 파드로 가도 무관.

---

## 1. 한눈에 — 무슨 서비스인가

- **입장(active)은 정원(`maxSessions`)만큼만.** 정원 차면 대기열(waiting)에 **선착순**(등록 시각순)으로 줄 세움.
- **자리 비면**(완료·이탈·타임아웃) → 백그라운드 루프가 대기열 **앞사람부터 자동 승격**.
- 사용자는 **`GET /position`을 폴링** — 순번(WAITING)·입장(ADMITTED)·만료(EXPIRED)를 한 응답으로 안다. 이 한 응답이 SSE의 세 push(STATS·ADMISSION·TIMEOUT)를 대체한다.
- 입장 승인은 **Kafka `admissions`**로 booking에 통지(좌석선택 게이트), 예매 완료는 **`bookings-completed`** 소비로 자리 반환. 서비스간 신호는 SSE 때와 동일.

---

## 2. 전체 흐름 — enter → 폴링 → 승격 → 타임아웃

```
클라(브라우저)                      queue 파드(stateless)               Redis
① POST /enter ────────────────▶ enter Lua(정원 확인, 원자) ──────────▶ active or waiting 등록
   ◀── 200 ADMITTED(입장) / 202 WAITING(rank+totalWaiting)
② 대기면: GET /position 반복 ──▶ 3-state 판정 + 순번·ETA 계산 ────────▶ ZSCORE·ZRANK
   (1-5초 적응형+jitter) ◀── { status, position, behind, etaSeconds }
   ※ 매 호출이 waiting_lastseen에 "나 살아있음" 도장을 찍음
③ status=ADMITTED 발견 → 폴링 중단 → 좌석선택으로

(서버 백그라운드 루프 — 사용자 요청과 무관하게 돎)
   승격(2초):        빈자리만큼 waiting 앞에서 active로 (promote Lua)
                     + Kafka admissions 발행(booking의 admitted 게이트 열기)
   active 타임아웃:   입장 후 SESSION_TIMEOUT 지난 active 제거 → 자리 반환
   waiting 타임아웃:  last-seen이 오래된(폴링 끊긴) 대기자 제거 → 자리 정리
   → 제거된 본인은 다음 폴링에서 EXPIRED로 발견
```

SSE와 결정적 차이: 서버→클라로 밀던 장치(연결·Hub·Pub/Sub 방송·bridge)가 전부 없다. 대신 "물어봄"에 답하는 장치(position 조회)와 "안 물어봄"을 감지하는 장치(last-seen)가 생겼다.

---

## 3. 파일 구조 — 각 파일의 역할

```
main.go            조립 — config 로드 → redis·kafka·rate 생성 → 프로세서 3개 goroutine → gin 라우트
config/config.go   env 설정(타입 박힌 Config 구조체). 기본값 = 로컬
handler/           Gin HTTP 입구 — 요청 파싱·검증·응답만
  admission.go       enter·position·leave·complete
  health.go          /health/live·/health/ready + shuttingDown 플래그(구 /health·force-fail은 2026-07-04 삭제)
processor/         백그라운드 ticker goroutine 3개 (§6)
  queue.go           승격 루프(2초) + rate 관측 + Kafka admissions 발행
  timeout.go         active 세션 타임아웃(§1-4a)
  waiting_timeout.go waiting 폴링(last-seen) 타임아웃(§1-4b)
redis/             go-redis + Lua — Redis 접근을 전부 여기 가둠
  client.go          go-redis 얇은 래퍼(풀·Ping)
  keys.go            키 빌더({movieId} 해시태그)
  enter.go           enter Lua — 정원 판정·재진입 정책(원자)
  promote.go         promote Lua — vacant 계산까지 Lua 안(P2)
  position.go        3-state 판정 + ZRANK 순번 계산
  timeout.go         active·waiting 만료 Lua(원자 제거)
  leave.go           active·waiting·lastseen 3키 동시 제거(멱등)
  query.go           조회 헬퍼(대상 영화·대기 인원)
metrics/rate.go    승격 처리율 EMA(α=0.3) — ETA 계산 재료(in-memory)
metrics/prom.go    promauto 계측 7벌(2026-07-05 신설) — 히스토그램·게이지·샘플러·/metrics :9091 서버(정본 = 설계서 1부 §7-B)
kafka/kafka.go     producer(admissions, RequireOne·재시도) + consumer(bookings-completed → 처리 후 커밋 → active ZREM) + 실패 카운터 2종
```

**핵심 원칙:** `handler`·`processor`는 **`redis` 패키지만** 보고 go-redis를 직접 안 만짐 → Redis 세부(Lua·키·드라이버)가 `redis/` 한 곳에 갇힘.
**폴링 전환으로 삭제됨:** `sse/`(hub·bridge)·`redis/pubsub.go`·`processor/stats.go` — push 파이프 통째로 소멸(디렉터리·파일 자체가 없다).

---

## 4. HTTP 엔드포인트 (`handler/`)

| 메서드 | 경로 | 기능 |
|---|---|---|
| POST | `/api/admission/enter` | 줄서기. 이미 active → 200 `ALREADY_ACTIVE`(자리 유지) / 자리 있음 → 200 `ADMITTED`(+Kafka 발행 — **발행 실패 시 보상 롤백 후 503 `RETRY_LATER`**, 2026-07-04) / 꽉 참 → 202 `WAITING`(rank·totalWaiting). **waiting 재진입 = 꼬리로 밀림**(ZREM 후 현재 시각으로 재ZADD) |
| GET | `/api/admission/position?movieId=&requestId=` | 폴링 순번 조회 → `{ status, position, behind, etaSeconds }` |
| POST | `/api/admission/leave` | 자발 이탈 — active·waiting·lastseen 전부 제거(멱등) |
| POST | `/api/admission/complete` | active 종료 → 자리 반환. 실제론 booking이 Kafka `bookings-completed`로 대신해 프론트는 안 씀 |
| GET | `/health/live` `/health/ready` | 헬스(live=생존 / ready=Redis PING + 종료 중 503). ※ 구 `/health`·`/health/force-fail-503`은 2026-07-04 삭제(미사용 라우트·상시 장애 스위치 — 설계서 1부 §2-A) |

**`/position`의 3-state — 판정 순서가 곧 로직 (`redis/position.go`):**
```
① ZSCORE active  에 있나 → ADMITTED  (승격됨 → 폴링 중단, 좌석으로)
② ZSCORE waiting 에 있나 → WAITING   (position=ZRANK+1, behind=ZCARD−position, eta=position÷rate)
③ 둘 다 없음             → EXPIRED   (타임아웃·이탈 → 영화목록으로)
```
- active를 먼저 봐야 승격 직후의 사람을 "없음"으로 오판하지 않고, EXPIRED가 있어야 큐에서 빠진 클라가 무한 폴링하지 않는다.
- `etaSeconds`는 승격 rate가 0이면(아직 승격 없음) -1 = "계산 중". `behind`·`etaSeconds`는 0도 유효값이라 omitempty 금지(꼴찌·곧 입장).

**신원 = requestId.** 로그인·세션 없음 — 프론트가 UUID를 만들어 localStorage에 저장하고 모든 요청에 담는다. 새로고침 정책: **active = 유지(ALREADY_ACTIVE) / waiting = 뒤로 밀림**(인터파크식, enter Lua가 명시적으로 민다).

---

## 5. 데이터 (Redis 키, `redis/keys.go`)

| 키 | 타입 | 용도 |
|---|---|---|
| `sessions:{movie}:active` | ZSet (member=requestId, score=입장ms) | 입장 세션 + 타임아웃 판정 |
| `sessions:{movie}:waiting` | ZSet (score=진입ms) | 대기 줄 — score순 = **선착순 FIFO** = 순번의 근거 |
| `waiting_lastseen:{movie}` | ZSet (score=마지막 폴링ms) | 폴링 생존 추적 — waiting 타임아웃의 재료 |
| `promoted_count:{movie}` | counter (INCRBY) | 누적 승격수 — rate·ETA 재료(promote Lua 안에서 증가, [2-2] 대시보드 재료 겸용) |
| `active_movies` / `waiting_movies` | Set | 트래픽 있는 영화 추적(루프가 돌 대상) |

- **waiting과 waiting_lastseen은 한 몸.** score 용도가 달라 분리했지만(waiting=진입순서 FCFS / lastseen=마지막 폴링 — 합치면 폴링할 때마다 순번이 밀림), **멤버는 항상 같이 움직인다**: enter에서 둘 다 ZADD, 빼는 모든 경로(승격·leave·타임아웃)에서 둘 다 ZREM — 같은 Lua로. 한쪽만 지우면 lastseen에 유령이 쌓인다.
- **폴링 전환으로 삭제**: `processed:{movie}`(클라 순위 자가계산 방송값 — push가 없으니 재료 불필요), `queue:notifications`(Pub/Sub 채널 — 방송 파이프 통째로 불필요).

---

## 6. 백그라운드 루프 3개 (`processor/`, ticker goroutine)

| 루프 | 주기(기본) | 하는 일 |
|---|---|---|
| QueueProcessor (승격) | 2초 | promote Lua 호출 — **vacant(=정원−ZCARD active) 계산을 Lua 안에서**(P2: Go에서 세면 멀티팟 동시 승격 때 초과입장 가능) → waiting 앞 n명을 active로(+lastseen 제거, promoted_count INCRBY) → 각 승격자마다 **Kafka admissions 발행** + rate 관측 |
| SessionTimeoutProcessor (active) | 10초 | score(입장시각) ≤ now−SESSION_TIMEOUT인 active를 원자 제거 → 자리 반환. 본인은 다음 폴링에서 EXPIRED 발견 |
| WaitingTimeoutProcessor (waiting) | 10초 | last-seen ≤ now−WAITING_TIMEOUT(기본 30초)인 대기자를 waiting·lastseen 둘 다에서 원자 제거 — **SSE "연결 끊김 감지"의 폴링판**(탭 닫음·튕김 → 폴링 멈춤 → 유예 후 자동 정리). 새로고침은 1초 내 재폴링이라 안 걸림 |

승격된 사람에게 일어나는 일 두 가지: ① 본인은 **다음 폴링에서 ADMITTED를 발견**(서버가 알려주지 않음) ② booking에 Kafka `admissions` 발행(admitted set — 없으면 좌석선택 403).

**승격 rate = EMA (`metrics/rate.go`).** 2초 루프라 순간값이 "0, 0, 3, 0…"으로 요동 → α=0.3 지수이동평균으로 완만하게(Prometheus `rate()`와 같은 발상). 0명 틱도 반영해 승격이 멈추면 rate가 0으로 수렴 → ETA "계산 중"(-1) 가드.

**Kafka (서비스간 신호 — SSE 때와 동일, `kafka/kafka.go`):**

| 토픽 | 방향 | 언제 |
|---|---|---|
| `admissions` | queue → booking | 입장 승인 시(enter 즉시입장 + 승격 루프). **ADMITTED일 때만 발행** — ALREADY_ACTIVE(새로고침 재진입)까지 발행하면 중복 |
| `bookings-completed` | booking → queue | 예매 완료 → consumer가 active ZREM(자리 반환) → 다음 틱에 승격 |

---

## 7. 설정 (env, `config/config.go`)

| 변수 | 기본 | 뜻 |
|---|---|---|
| `PORT` | 8090 | 리슨 포트. compose에선 게이트웨이 :8088 경유가 정상 경로, 직노출은 호스트 **18090**(8090은 Wondershare 점유로 우회) |
| `REDIS_HOST`/`REDIS_PORT` | localhost/6379 | Redis 주소 |
| `REDIS_PASSWORD` | (빈 값) | prod에서 주입 |
| `MAX_SESSIONS` | 2 | 입장 정원(데모 소정원 — 동적정원은 2-2) |
| `SESSION_TIMEOUT` | 60 | active 세션 수명(초). 데모 60(실운영 600) — 좌석락 45s < 세션 60s 정합 |
| `QUEUE_PROCESS_INTERVAL` | 2000 | 승격 주기(ms) |
| `PROCESSING_BATCH_SIZE` | 100 | 한 번에 승격 상한(안전밸브) |
| `SESSION_CLEANUP_INTERVAL` | 10000 | active 만료 검사 주기(ms) |
| `WAITING_TIMEOUT` | 30 | 폴링 끊긴 대기자 evict 임계(초, 폴링 주기보다 넉넉히) |
| `WAITING_CLEANUP_INTERVAL` | 10000 | waiting 만료 검사 주기(ms) |
| `KAFKA_BROKER` | localhost:9092 | Kafka 주소(admissions·bookings-completed) |
| `REDIS_POOL_SIZE` | 미설정(=라이브러리 기본) | Redis 커넥션 풀 크기 — 값은 부하 실측 후 확정(설계서 1부 §1) |
| `METRICS_PORT` | 9091 | /metrics 전용 포트(앱 포트와 분리 — 설계서 1부 §5-D). 로컬 확인 = 호스트 19091 |

단독 기동: `./run.sh`(WSL, 네이티브 Redis 필요). 풀스택은 저장소 루트에서 `docker compose up --build -d` → 브라우저 http://localhost:8088.

---

## 8. 스코프 — [2-1] 지금 / [2-2] 인프라 때

**[2-1] = 지금(로컬 앱, 단일 인스턴스).** 루프는 리더선출 없이 그냥 돌고, 순번은 매 요청 ZRANK로 충분(부하 없음). 로그는 `log.Printf`만.

**[2-2] = 인프라 배포 때 추가(멀티팟·부하·관측 전제):**
- **리더 선출** (`SET queue:leader NX EX`) — 멀티팟에서 승격·타임아웃·스냅샷 루프는 리더 한 파드만(중복 방지).
- **순번 캐싱** (`position_cache:{movie}` 스냅샷) — 10만이 초당 폴링하면 ZRANK 폭격 → 리더가 1-2초마다 스냅샷을 굽고 폴링은 O(1) 읽기. 단 ADMITTED/EXPIRED **판정만은 캐시 말고 ZSCORE 직접**(입장 발견이 지연되면 안 됨).
- ~~관측 계측(promauto)~~ → **✔완료(2026-07-05)**: metrics/prom.go 7벌 + /metrics :9091(설계서 1부 §7-B 정본). 구조화 로그(slog JSON)·트레이스(OTel)만 남음 — Loki·Tempo 붙일 때.
- ~~graceful shutdown·automaxprocs~~ → **✔완료(2026-07-04)**. Sentinel-aware만 남음(B §2 확정 후).
- ~~Kafka 발행 실패 재시도~~ → **✔완료(2026-07-04)**: 재시도+보상 롤백+RETRY_LATER(정직한 실패) + 실패 카운터 노출(07-05).

부하 축은 SSE의 "유지되는 연결"에서 **RPS**로 바뀌었다 — 파드는 요청 사이에 아무것도 안 들고, 스케일은 표준 HPA(CPU/RPS)로 충분. Redis가 유일한 부하 지점이라 방어가 위 캐싱이다(상세 = 올인원 §1-7).
