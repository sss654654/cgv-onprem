# cgv-onprem — 폴리글랏 티켓팅 MSA

티켓팅(생중계 좌석 예매) 대기열 시스템. 입장 통제(queue)와 예매(booking)를 서로 다른 언어로 나누고, 둘을 Kafka 이벤트로만 연결해 서비스 간 장애를 격리한다. 온프레미스 k3s 클러스터(dev)에 GitOps로 배포·운영 중이고, 로컬 검증은 `docker-compose`로 한다.

- **queue** (Go) — 입장 정원·대기 순번·승격. stateless 폴링 대기열
- **booking** (Java/Spring) — 좌석 선점·결제·확정. 결제·정합성을 다루는 트랜잭션 서비스
- **frontend** (바닐라 SPA + nginx) — 브라우저 진입점 겸 게이트웨이. 화면은 티케팅 대기열 시뮬레이터다 — 방문자가 가상 관객을 투입해 대기열이 도는 것을 지켜보고 직접 예매까지 해본다

> **이 GitHub 저장소는 읽기용 미러다.** 실제 작업은 로컬 작업 트리와 셀프호스트 GitLab(데스크탑 Docker)에서 하고, GitLab의 push mirroring이 여기로 자동 반영한다. MR·CI 파이프라인·컨테이너 레지스트리는 GitLab 쪽에 있어 이 저장소에는 결과(커밋)만 보인다.
>
> 인프라(k3s·GitOps·관측)는 [cgv-infra](https://github.com/sss654654/cgv-infra), 구축 과정 기록은 [블로그 연재](https://zed6740.tistory.com/category/HomeLab)에 있다.

---

## 배경

KBO 야구의 온라인 예매가 폭주해 예매 서버가 수요를 받아내지 못한 실제 사건을 모델로 한다. 보도·후기에 따르면 예매 버튼이 활성화되지도 못한 채 대기 인원이 16만 명에 이르렀고, 좌석은 몇 분 만에 전량 매진됐다. 추측건대 원인은 "대기열·용량 산정 부실 → 수요가 백엔드를 직격"한 것이다. (기업의 공식 장애 발표는 없어 정황은 후기·보도로 확인된다.) 이에 CGV가 극장에서 야구 생중계 예매 서비스를 지원하기로 했다.


이 사건을 축소한 구성이며, 아래 세 가지로 이뤄진다.

- **queue**가 정원 게이트로 booking 처리량만큼만 입장을 허용한다.
- **booking**은 게이트 뒤에서 좌석·결제를 정합성 있게 처리한다.
- 둘을 **Kafka로 격리**해 한쪽의 부하·장애가 다른 쪽에 전파되지 않는다.

설계 목표도 이 사건 규모에서 역산한다 — 동시 대기 10만(16만 기준), 좌석 약 4,000석(지점 여럿 × 관당 200석), 오픈 직후 enter 버스트. 이 숫자가 이후 Pod 수·리소스 사이징의 출발점이 된다.

---

## 아키텍처

```
                       브라우저  (http://localhost:8088)
                               │
                     ┌─────────▼──────────┐
                     │  frontend (nginx)  │   정적 SPA + 게이트웨이
                     └────┬──────────┬────┘   (k3s에선 Traefik Ingress)
          /api/admission/*│          │/api/*
                     ┌────▼────┐  ┌──▼──────┐
                     │  queue  │  │ booking │
                     │  (Go)   │  │ (Java)  │
                     └──┬───┬──┘  └──┬───┬──┘
                        │   │        │   │
             (큐 상태) Redis│        │   Redis (좌석락·admitted)
                            │        │
                        ┌───▼────────▼───┐
                        │      Kafka      │
                        │  admissions ───────▶  (queue → booking : 입장 승인)
                        │  admissions-revoked ▶ (queue → booking : 입장 회수)
                        │  ◀─── bookings-completed  (booking → queue : 자리 반환)
                        └────────────────┘        booking → MySQL (확정 예매)
```

- **게이트웨이가 경로로 서비스를 나눈다**: 브라우저는 한 주소만 보고, `/api/admission/*`은 queue, 나머지 `/api/*`은 booking으로 간다.
- **Kafka 토픽이 닫힌 순환을 이룬다**: queue가 승격하면 `admissions`로 알리고(booking이 입장 인증 채움), booking이 예매를 끝내면 `bookings-completed`로 알린다(queue가 자리 반환). 자리를 거둘 때는 `admissions-revoked`로 인증을 지운다. 직접 호출이 없어 한쪽이 죽어도 다른 쪽으로 전파되지 않는다.
- **저장소가 성격을 가른다**: queue는 휘발성 큐라 Redis만, booking은 영속 정합성이라 MySQL(확정) + Redis(임시 점유).

---

## 서비스별 문서

각 서비스의 코드 구조·흐름·관측·배포 특징은 개별 README에 있다.

| 서비스 | 언어 | 역할 | 문서 |
|---|---|---|---|
| queue | Go | 입장 통제(정원·순번·승격) | [queue-go/README.md](queue-go/README.md) |
| booking | Java/Spring | 좌석·결제·확정 | [booking/README.md](booking/README.md) |
| frontend | 바닐라 JS + nginx | SPA + 게이트웨이 | [frontend/README.md](frontend/README.md) |

---

## 왜 나눴나

- **독립 스케일**: queue는 폴링 RPS(CPU 바운드, stateless라 선형 증설), booking은 트랜잭션 처리량(공유 MySQL이라 비선형). 부하 축이 달라 함께 늘릴 이유가 없다.
- **장애 격리**: booking(DB가 무겁고 크래시 가능)이 죽어도 queue(대기열 전체 보유)는 유지돼야 한다. Kafka 비동기라 승격이 booking을 기다리지 않는다.
- **경계 분리**: 입장/정원과 결제/좌석은 저장소도 실패 도메인도 다르다.

언어를 다르게 쓴 것(폴리글랏)은 나눈 뒤의 최적화다. 서비스를 나눈 근본 이유는 언어와 무관하다.

---

## 사용자 여정

```
영화 클릭 → enter → (정원 여유?)
   200 ADMITTED ─────────────────────────────────┐
   202 WAITING → 순번 폴링 → 승격(Kafka) → 폴링이 ADMITTED 발견 ─┤
                                                              ▼
                            회차 선택 → 좌석 선점 → 결제 → 확정
                                                              │
              bookings-completed(Kafka) → queue 자리 반환 → 다음 대기자 승격
```

비동기가 화면에 드러나는 유일한 지점은 입장 직후 좌석선택 403 경쟁이며, 프론트가 전환 유예 + 재시도로 처리한다.

---

## 실행

```bash
cd cgv-onprem
docker compose up --build -d      # redis · mysql · kafka · queue · booking · frontend
```

- 브라우저: **http://localhost:8088**
- 첫 빌드는 느리다(Go 빌드 + booking Maven 빌드). Kafka(KRaft) 기동에 20~30초 걸리며 그동안 queue/booking이 재연결을 재시도한다.
- 정원이 `MAX_SESSIONS=2`라 처음 2명만 즉시 입장, 이후는 대기열로 간다(탭 여러 개 또는 프론트 시뮬레이터로 확인).
- E2E 스크립트: `./e2e2.sh`
- 끄기: `docker compose down` (데이터까지: `-v`)

---

## 현재 상태

- **k3s dev 클러스터에서 운영 중**: push → GitLab CI 5단 게이트(check·test·build·scan·publish) → 불변 태그(`dev-<파이프라인>-<커밋>`) 이미지 → argocd-image-updater가 태그를 GitOps 저장소에 write-back → Argo CD 롤아웃. 배포 선언·클러스터 구성은 [cgv-infra](https://github.com/sss654654/cgv-infra)에 있다.
- **관측**: metric·log·trace 세 축이 세 서비스에 계측돼 있고, 클러스터의 LGTM 스택(Mimir·Loki·Tempo + Alloy·Grafana)으로 흐른다. 로컬 compose에는 수집 스택이 없어 exporter가 조용히 쉰다.
- **세 축이 서로 이어져 있다**: p99 그래프의 exemplar를 누르면 그 요청의 트레이스로, 트레이스에서 그 요청이 남긴 로그로 건너간다. 트레이스에는 HTTP·Kafka뿐 아니라 SQL 구간과 Redis 명령이 span으로 들어 있어, 예매 확정 한 건이 21개 span으로 쪼개진다. Kafka를 넘는 경로(입장 승격 → 인증 발급, 예매 확정 → 자리 반환)도 한 트레이스로 이어진다.
- **트레이스 표본**: booking·queue 모두 전 요청(`OTEL_TRACES_SAMPLER_ARG=1.0`)이고 프로브만 만들지 않는다. queue 폴링이 요청의 97%라 한동안 1%로 두었는데, 표본을 올려도 exemplar 개수가 늘지 않았다 — 스크레이프(15초)마다 히스토그램 버킷당 하나가 한도라 표본이 아니라 그 한도가 개수를 정한다. 느린 요청을 훑는 수단을 Tempo 검색(TraceQL)으로 옮기고 표본을 전부로 올렸다.
- **부하로 스펙을 정했다**: 판을 두 번에 나눠 돌렸다 — 클러스터 안에서 21회, 인터넷에 공개한 뒤 같은 경로로 13회. 정원·타임아웃·CPU/메모리 limit·커넥션 풀이 그 판에서 나왔고, "목표를 넣으면 스펙이 나오는" 계수 열 개를 남겼다(예: `booking CPU = 0.007 + 0.0258 × 확정/초`). 값보다 그 근거가 산출물이다.
- **인터넷에 공개돼 있다**: [ticket.subinhong.dev](https://ticket.subinhong.dev). 대기열·예매 API는 무인증이고 rate limit도 없다 — 익명 접속을 받는 것이 이 서비스의 목적이라 접수 단계에서 거를 수 없어서다. 좌석 오염은 주기 초기화가 받고 대량 트래픽은 CDN 엣지가 앞에서 받는다.
- **부하가 드러낸 것을 앱에서 고쳤다** — 네 건 모두 자원을 늘려 푼 것이 아니라 코드로 풀었다.

  | 무너진 것 | 원인 | 조치 |
  |---|---|---|
  | 입장 인증 지연 p99 9.94초 | `admissions` 소비가 레코드 건별이라 상한 초당 28건 | 배치 리스너 + Redis 파이프라인 |
  | 앱 전체 정지 · 5xx 초당 467건 | 회차 집계 캐시가 만료되는 순간 대기 중인 전원이 동시에 계산 | single-flight — 만료를 본 요청 중 하나만 계산하고 나머지는 직전 값을 받는다 |
  | 인증 지연 p99 19.86초 | 로비의 좌석 현황판 폴링이 booking CPU를 차지해 Kafka 리스너가 순서를 못 받음 | 응답에 `Cache-Control`을 붙여 CDN 엣지가 받게 함 |
  | 영화 목록이 booking을 때림 | 대기열 게이트 밖 경로라 인원에 정비례 | nginx 캐시 10초 + 캐시 잠금 |

### 확정 스펙 (부하 실측)

```
MAX_SESSIONS    1,000        동시에 좌석 화면에 들일 인원.  상한은 자원이 아니라 좌석 4,000이 정한다
SESSION_TIMEOUT   300초       회차 조회 + 좌석 선택 + 결제의 합
SEAT_LOCK_TTL     180초       좌석 선택에서 결제 진입까지
ADMITTED_TTL      600초       회수 이벤트 유실 시 최후 방어.  세션의 2배
승격 배치 / 주기   25 / 0.5초   상한(초당 50명)은 100/2초와 같고 뭉텅이만 4분의 1
DB_POOL_SIZE       30         10일 때 대기 397건
booking          2코어 · limit 1,536Mi · heap 상한 768Mi
queue            4 replicas · limit 1코어 / 256Mi
```

**세 시간 값의 순서가 깨지면 정상 사용자가 실패한다** — 좌석 180 < 세션 300 < 인증 600.
좌석을 고르는 중에 자리가 회수되면 선택이 403이 된다.

로컬 `docker-compose`는 다른 값이다(`MAX_SESSIONS=2`). 대기열 동작을 바로 보려고 낮춰 뒀다.
공개 사이트의 정원도 위 실측값보다 낮다 — 1,000이면 방문자가 가상 관객을 넣어도 전원이 즉시
입장해 대기열이 화면에 안 나타난다.

---

## 저장소 레이아웃

```
cgv-onprem/
├── queue-go/            입장 통제 서비스 (Go)
├── booking/             예매 서비스 (Java/Spring)
├── frontend/            SPA + nginx 게이트웨이
├── docker-compose.yml   로컬 풀스택 정의
├── e2e2.sh              게이트웨이 통과 E2E 스크립트
└── .gitignore
```
