package handler

import (
	"context"
	"log/slog"
	"net/http"
	"regexp"
	"time"

	"github.com/gin-gonic/gin"

	"cgv-onprem/queue-go/metrics"
	"cgv-onprem/queue-go/redis"
)

// idPattern = movieId·requestId 허용 형식. 두 값은 Redis 키 이름·메트릭 라벨·발행 저널
// member('|' 구분)에 그대로 들어간다 — 자유 문자열을 받으면 요청 한 번으로 키와 시계열이
// 늘어나고 지울 수단이 없다. 클라이언트 발급 UUID(36자)를 덮는 64자 상한.
var idPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// rejectInvalidIDs = 형식 밖 id가 있으면 400을 쓰고 true를 반환한다.
func rejectInvalidIDs(c *gin.Context, ids ...string) bool {
	for _, id := range ids {
		if !idPattern.MatchString(id) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "movieId, requestId 형식이 올바르지 않습니다(영숫자·_·-, 최대 64자)"})
			return true
		}
	}
	return false
}

// AdmissionPublisher = 입장 시 서비스간(Kafka) 입장 이벤트 발행. main이 주입(kafka producer).
type AdmissionPublisher interface {
	PublishAdmission(ctx context.Context, requestID, movieID string) error
}

// publishTimeout = enter 경로의 발행 상한. Writer의 시도당 타임아웃(1s)×재시도(3)와 백오프를
// 덮되, 요청 스레드를 오래 잡지 않는 값. 여기서 끊겨도 저널이 남아 스윕이 이어받는다.
const publishTimeout = 6 * time.Second

// Admission = 대기열 진입(enter)·순번조회(position)·이탈(leave)·종료(complete) 핸들러.
// 폴링 재설계 — SSE stream/Hub는 제거, position 폴링으로 대체.
type Admission struct {
	rdb         *redis.Client
	maxSessions int64
	publisher   AdmissionPublisher
	rate        *metrics.RateProvider
	adminToken  string
}

func NewAdmission(rdb *redis.Client, maxSessions int64, publisher AdmissionPublisher, rate *metrics.RateProvider, adminToken string) *Admission {
	return &Admission{rdb: rdb, maxSessions: maxSessions, publisher: publisher, rate: rate, adminToken: adminToken}
}

func (a *Admission) Register(r *gin.Engine) {
	r.POST("/api/admission/enter", a.enter)
	r.GET("/api/admission/position", a.position)   // 폴링 순번 조회
	r.GET("/api/admission/stats", a.stats)         // 전체 현황 집계(시뮬레이터 화면용, 읽기 전용)
	r.POST("/api/admission/leave", a.leave)        // 대기열 이탈(active·waiting 제거)
	r.POST("/api/admission/complete", a.complete)  // active 종료 → 자리 반환
	// 대기열 초기화 — 토큰을 주입한 배포에만 존재한다. 라우트를 조건부로 다는 이유:
	// 토큰이 없을 때 핸들러가 403을 주는 방식이면 "그 API가 있다"는 사실 자체가 남는다.
	if a.adminToken != "" {
		r.POST("/api/admission/reset", a.reset)
	}
}

// requireAdmin = X-Admin-Token 검사. 통과 못 하면 응답을 쓰고 false.
// 상수시간 비교까지 갈 값은 아니지만(토큰 길이가 고정이 아님), 실패 응답에 이유를 남기지 않는다.
func (a *Admission) requireAdmin(c *gin.Context) bool {
	if a.adminToken == "" || c.GetHeader("X-Admin-Token") != a.adminToken {
		c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
		return false
	}
	return true
}

// reset = POST /api/admission/reset. 한 영화의 대기열 상태를 비운다.
// 데모가 오래 돌아 숫자가 쌓였을 때 되돌리는 경로 — 예매 기록·좌석은 booking 쪽 리셋이 맡는다.
func (a *Admission) reset(c *gin.Context) {
	if !a.requireAdmin(c) {
		return
	}
	var req struct {
		MovieID string `json:"movieId" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "movieId는 필수입니다"})
		return
	}
	if rejectInvalidIDs(c, req.MovieID) {
		return
	}
	if err := a.rdb.Reset(c.Request.Context(), req.MovieID, time.Now().Unix()); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		return
	}
	slog.WarnContext(c.Request.Context(), "대기열 초기화 실행", "movie", req.MovieID)
	c.JSON(http.StatusOK, gin.H{"status": "RESET", "movieId": req.MovieID})
}

// enterRequest = movieId + requestId(클라 생성 UUID, localStorage 지속).
type enterRequest struct {
	MovieID   string `json:"movieId" binding:"required"`
	RequestID string `json:"requestId" binding:"required"`
}

// enterResponse = ADMITTED면 rank/total 생략(omitempty), WAITING이면 포함.
type enterResponse struct {
	Status       string `json:"status"`
	RequestID    string `json:"requestId"`
	Rank         int64  `json:"rank,omitempty"`
	TotalWaiting int64  `json:"totalWaiting,omitempty"`
}

// enter = POST /api/admission/enter. ADMITTED → 200, WAITING → 202(Accepted).
func (a *Admission) enter(c *gin.Context) {
	var req enterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "movieId, requestId는 필수입니다"})
		return
	}
	if rejectInvalidIDs(c, req.MovieID, req.RequestID) {
		return
	}

	now := time.Now().UnixMilli()
	res, err := a.rdb.Enter(c.Request.Context(), req.MovieID, req.RequestID, a.maxSessions, now)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		return
	}

	resp := enterResponse{Status: res.Code, RequestID: req.RequestID}
	if res.Status == 2 { // waiting 경로
		resp.Rank = res.Rank
		resp.TotalWaiting = res.Count
		c.JSON(http.StatusAccepted, resp)
		return
	}
	// active 경로(즉시 입장) → booking이 알도록 Kafka 입장 이벤트 발행.
	// 안 하면 booking admitted set에 없어 좌석선택이 403.
	// ADMITTED(첫 입장)일 때만 — ALREADY_ACTIVE(새로고침 재진입)까지 발행하면 중복.
	if a.publisher != nil && res.Code == "ADMITTED" {
		// 발행에는 요청 ctx를 쓰지 않는다. 클라이언트가 응답을 기다리다 끊으면(모바일 전환·
		// k6 타임아웃) 요청 ctx가 취소되는데, kafka-go는 브로커에 이미 들어간 뒤에도 그 취소를
		// 에러로 돌려준다 — 실패로 오판해 되돌리면 booking엔 입장이 들어갔는데 queue만 자리를
		// 뺏는 유령 입장자가 생긴다. 발행은 클라이언트 수명과 분리한다.
		pubCtx, cancel := context.WithTimeout(context.WithoutCancel(c.Request.Context()), publishTimeout)
		err := a.publisher.PublishAdmission(pubCtx, req.RequestID, req.MovieID)
		cancel()
		if err != nil {
			// 되돌리지 않는다. Enter가 active 등록과 같은 원자 실행으로 발행 대기 저널에
			// 남겨뒀으므로, 스윕 루프가 이어서 발행한다. 지금 되돌리면 위의 유령 입장자 경로가 열린다.
			// 클라이언트에는 재시도를 안내한다 — Enter는 멱등이라 재시도하면 ALREADY_ACTIVE로 통과하고,
			// 그사이 스윕이 발행을 끝낸다.
			slog.WarnContext(c.Request.Context(), "enter 발행 실패(저널 보존 — 스윕이 재발행)", "req", req.RequestID, "movie", req.MovieID, "err", err)
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"status": "RETRY_LATER",
				"error":  "입장 처리가 지연되고 있습니다. 잠시 후 다시 시도해주세요.",
			})
			return
		}
		// 발행이 끝났으니 저널에서 지운다. 실패해도 스윕이 한 번 더 보낼 뿐이고
		// booking의 admissions 소비는 SADD라 멱등이다.
		member := redis.PendingMember(redis.KindAdmit, redis.ReasonEnter, req.RequestID)
		if err := a.rdb.ClearPending(c.Request.Context(), req.MovieID, []string{member}); err != nil {
			slog.WarnContext(c.Request.Context(), "enter 저널 정리 실패(중복 발행 가능, 소비는 멱등)", "req", req.RequestID, "err", err)
		}
	}
	c.JSON(http.StatusOK, resp) // active 경로(ADMITTED/ALREADY_ACTIVE)
}

// positionResponse = 폴링 응답. 대기화면 표시용.
type positionResponse struct {
	Status     string `json:"status"`             // WAITING / ADMITTED / EXPIRED
	Position   int64  `json:"position,omitempty"` // 1-based 순번(WAITING). 0 불가라 omitempty 안전
	Behind     int64  `json:"behind"`             // 내 뒤 인원(WAITING). 0도 유효값(맨 뒤) → omitempty 금지
	EtaSeconds int64  `json:"etaSeconds"`         // 예상 대기(초). -1=알수없음. 0도 유효값(곧 입장) → omitempty 금지
}

// position = GET /api/admission/position?movieId=&requestId=
// 3-state 판정(ZSCORE active→ADMITTED / ZRANK waiting→WAITING / 둘다없음→EXPIRED).
func (a *Admission) position(c *gin.Context) {
	movieID := c.Query("movieId")
	requestID := c.Query("requestId")
	if movieID == "" || requestID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "movieId, requestId 필수"})
		return
	}
	if rejectInvalidIDs(c, movieID, requestID) {
		return
	}

	now := time.Now().UnixMilli()
	res, err := a.rdb.Position(c.Request.Context(), movieID, requestID, now)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		return
	}

	resp := positionResponse{Status: res.Status}
	if res.Status == "WAITING" {
		resp.Position = res.Position
		resp.Behind = res.Behind
		resp.EtaSeconds = -1
		if a.rate != nil {
			resp.EtaSeconds = a.rate.ETASeconds(c.Request.Context(), movieID, res.Position)
		}
	}
	c.JSON(http.StatusOK, resp)
}

// stats = GET /api/admission/stats?movieId=
// 시뮬레이터 화면의 전체 현황(입장 n/정원 · 대기 n · 누적 승격). 상태를 바꾸지 않는다.
func (a *Admission) stats(c *gin.Context) {
	movieID := c.Query("movieId")
	if movieID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "movieId 필수"})
		return
	}
	if rejectInvalidIDs(c, movieID) {
		return
	}
	active, waiting, promoted, err := a.rdb.Stats(c.Request.Context(), movieID)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		return
	}
	// etaNext = 지금 새로 들어오는 사람의 예상 대기(대기열 끝 순번 기준). rate 미형성이면 -1.
	etaNext := int64(-1)
	if a.rate != nil {
		etaNext = a.rate.ETASeconds(c.Request.Context(), movieID, waiting+1)
	}
	c.JSON(http.StatusOK, gin.H{
		"active":         active,
		"waiting":        waiting,
		"promotedTotal":  promoted,
		"capacity":       a.maxSessions,
		"etaNextSeconds": etaNext,
	})
}

// idRequest = leave/complete 공용 요청(movieId+requestId).
type idRequest struct {
	MovieID   string `json:"movieId" binding:"required"`
	RequestID string `json:"requestId" binding:"required"`
}

// leave = POST /api/admission/leave. 대기열에서 이탈(active·waiting·lastseen 제거).
func (a *Admission) leave(c *gin.Context) {
	var req idRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "movieId, requestId는 필수입니다"})
		return
	}
	if rejectInvalidIDs(c, req.MovieID, req.RequestID) {
		return
	}
	// active에 있던 사용자면 회수 이벤트가 발행 대기 저널에 남는다(스윕이 booking에 통보).
	if err := a.rdb.Leave(c.Request.Context(), req.MovieID, req.RequestID, time.Now().UnixMilli()); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "LEFT", "requestId": req.RequestID})
}

// complete = POST /api/admission/complete. active 세션 종료 → 자리 반환(→③이 다음 대기자 승격).
func (a *Admission) complete(c *gin.Context) {
	var req idRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "movieId, requestId는 필수입니다"})
		return
	}
	if rejectInvalidIDs(c, req.MovieID, req.RequestID) {
		return
	}
	// 사용자가 부른 자리 반환이라 booking의 admitted는 아직 그 사용자를 들고 있다
	// → 회수 이벤트를 저널에 남겨 스윕이 통보한다. (booking의 예매 확정으로 끝난 경우는
	// bookings-completed 소비 경로가 CompleteActive를 쓴다 — 그쪽은 booking이 이미 지웠다.)
	removed, err := a.rdb.ReleaseActive(c.Request.Context(), req.MovieID, req.RequestID, redis.ReasonComplete, time.Now().UnixMilli())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "COMPLETED", "requestId": req.RequestID, "removed": removed})
}
