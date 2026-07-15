# cgv-onprem — 폴리글랏 티켓팅 MSA

티켓팅(생중계 좌석 예매) 대기열 시스템. 입장 통제(queue)와 예매(booking)를 서로 다른 언어로 나누고, 둘을 Kafka 이벤트로만 연결해 서비스 간 장애를 격리한다. 온프레미스 k3s 배포를 대상으로 하며, 현재는 로컬 `docker-compose`로 검증하는 단계다.

- **queue** (Go) — 입장 정원·대기 순번·승격. stateless 폴링 대기열
- **booking** (Java/Spring) — 좌석 선점·결제·확정. 결제·정합성을 다루는 트랜잭션 서비스
- **frontend** (바닐라 SPA + nginx) — 브라우저 진입점 겸 게이트웨이

---

## 배경

CGV가 극장에서 KBO 야구를 생중계 상영했을 때, 그 극장 좌석의 온라인 예매가 폭주해 예매 서버가 수요를 받아내지 못한 실제 사건을 모델로 한다. 보도·후기에 따르면 예매 버튼이 활성화되지도 못한 채 대기 인원이 16만 명에 이르렀고, 좌석은 몇 분 만에 전량 매진됐다. 원인은 "대기열·용량 산정 부실 → 수요가 백엔드를 직격"한 것이다. (기업의 공식 장애 발표는 없어 정황은 후기·보도로 확인된다.)

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
                        │  ◀─── bookings-completed  (booking → queue : 자리 반환)
                        └────────────────┘        booking → MySQL (확정 예매)
```

- **게이트웨이가 경로로 서비스를 나눈다**: 브라우저는 한 주소만 보고, `/api/admission/*`은 queue, 나머지 `/api/*`은 booking으로 간다.
- **Kafka 두 토픽이 닫힌 순환을 이룬다**: queue가 승격하면 `admissions`로 알리고(booking이 입장 인증 채움), booking이 예매를 끝내면 `bookings-completed`로 알린다(queue가 자리 반환). 직접 호출이 없어 한쪽이 죽어도 다른 쪽으로 전파되지 않는다.
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

- **로컬 검증 완료**: 6개 컨테이너로 닫힌 순환(go → Kafka → java → Kafka → go)을 curl 왕복으로 확인. queue·booking Docker 빌드 통과.
- **관측 배선 완료(코드)**: metric·log·trace 세 축이 세 서비스에 들어가 있으나, 수집·저장 스택(LGTM)은 아직 없어 로컬에선 흐르지 않는다.
- **다음(k3s)**: 클러스터 배포·오토스케일·관측 스택·부하테스트. 매니페스트·차트는 별도 GitOps repo에서 관리한다.

배포 시 되돌릴 데모값: `MAX_SESSIONS` 2 → 측정값 · `SESSION_TIMEOUT` 60 → 600 · `SEAT_LOCK_TTL` 45 → 300 · `VIRTUAL_THREADS` false → true.

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
