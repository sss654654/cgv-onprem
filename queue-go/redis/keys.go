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

// 트래픽 있는 영화 추적용 Set — 승격·타임아웃 루프가 이걸로 대상 영화를 안다.
const (
	ActiveMoviesKey  = "active_movies"
	WaitingMoviesKey = "waiting_movies"
)
