package redis

// 키 빌더 — 폴링 재설계 기준(백엔드서비스-올인원.md §1-6).
// {movieId} 해시태그는 Cluster CROSSSLOT 대비 흔적(Non-Cluster엔 무효, 이름의 일부).

func ActiveKey(movieID string) string {
	return "sessions:{" + movieID + "}:active" // ZSet, member=requestId, score=입장ms
}

func WaitingKey(movieID string) string {
	return "sessions:{" + movieID + "}:waiting" // ZSet, score=진입순서ms=선착순 FIFO(순번용)
}

// WaitingLastseenKey = 폴링 생존 추적(§1-4b). ZSet, score=마지막 폴링시각ms.
// waiting과 별도 키 — waiting score(진입순서)를 폴링시각으로 덮으면 FCFS가 깨지므로 분리.
func WaitingLastseenKey(movieID string) string {
	return "waiting_lastseen:{" + movieID + "}"
}

// PromotedCountKey = 누적 승격수(§1-3). counter(INCRBY). rate·ETA 계산용.
// (SSE의 processed=클라 자가계산 방송값과는 다른 값 — 이건 rate 계산용.)
func PromotedCountKey(movieID string) string {
	return "promoted_count:{" + movieID + "}"
}

// PendingKey = Kafka 발행 대기 저널. ZSet, score=상태변경시각ms,
// member="<kind>|<reason>|<requestId>" (kind: A=입장 통보, R=입장 회수).
// 상태 변경(승격·입장·타임아웃·이탈)과 같은 Lua 안에서 기록되고, 발행이 성공하면 제거된다.
// 발행 전에 파드가 죽어도 이 저널은 Redis에 남아, 살아 있는 파드의 sweep이 이어서 발행한다
// (승격 Lua 성공 → 발행 전 즉사 시 승격자 명단이 사라지던 경로를 닫는다).
func PendingKey(movieID string) string {
	return "pending_events:{" + movieID + "}"
}

// 트래픽 있는 영화 추적용 Set — 승격·타임아웃 루프가 이걸로 대상 영화를 안다.
const (
	ActiveMoviesKey  = "active_movies"
	WaitingMoviesKey = "waiting_movies"
)

// PromotePauseKey = 발행 실패 후 승격을 쉬는 구간의 공유 플래그(TTL로 자동 해제).
// 파드 로컬 변수로 두면 replica가 2 이상일 때 다른 파드가 계속 승격해 중단 효과가 부분적으로만 걸린다.
const PromotePauseKey = "queue:promote_pause"
