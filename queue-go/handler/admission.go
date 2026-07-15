package handler

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"cgv-onprem/queue-go/metrics"
	"cgv-onprem/queue-go/redis"
)

// AdmissionPublisher = 입장 시 서비스간(Kafka) 입장 이벤트 발행. main이 주입(kafka producer).
type AdmissionPublisher interface {
	PublishAdmission(ctx context.Context, requestID, movieID string) error
}

// Admission = 대기열 진입(enter)·순번조회(position)·이탈(leave)·종료(complete) 핸들러.
// 폴링 재설계(백엔드서비스-올인원.md §1) — SSE stream/Hub는 제거, position 폴링으로 대체.
type Admission struct {
	rdb         *redis.Client
	maxSessions int64
	publisher   AdmissionPublisher
	rate        *metrics.RateTracker
}

func NewAdmission(rdb *redis.Client, maxSessions int64, publisher AdmissionPublisher, rate *metrics.RateTracker) *Admission {
	return &Admission{rdb: rdb, maxSessions: maxSessions, publisher: publisher, rate: rate}
}

func (a *Admission) Register(r *gin.Engine) {
	r.POST("/api/admission/enter", a.enter)
	r.GET("/api/admission/position", a.position)   // 폴링 순번 조회(§1-2)
	r.POST("/api/admission/leave", a.leave)        // 대기열 이탈(active·waiting 제거)
	r.POST("/api/admission/complete", a.complete)  // active 종료 → 자리 반환
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
		if err := a.publisher.PublishAdmission(c.Request.Context(), req.RequestID, req.MovieID); err != nil {
			// (b) 정직한 실패(설계서 1부 §3-A, 필수 5): booking이 모르는 ADMITTED는
			// 403 + 자리 점유를 낳으므로, active에 넣은 걸 되돌리고(보상 롤백) 재시도를 안내한다.
			// 차례·정원 보존.
			if _, rbErr := a.rdb.CompleteActive(c.Request.Context(), req.MovieID, req.RequestID); rbErr != nil {
				slog.ErrorContext(c.Request.Context(), "enter 보상 롤백 실패(60s 타임아웃이 회수)", "req", req.RequestID, "movie", req.MovieID, "err", rbErr)
			}
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"status": "RETRY_LATER",
				"error":  "입장 처리가 지연되고 있습니다. 잠시 후 다시 시도해주세요.",
			})
			return
		}
	}
	c.JSON(http.StatusOK, resp) // active 경로(ADMITTED/ALREADY_ACTIVE)
}

// positionResponse = 폴링 응답(§1-2). 대기화면 표시용.
type positionResponse struct {
	Status     string `json:"status"`             // WAITING / ADMITTED / EXPIRED
	Position   int64  `json:"position,omitempty"` // 1-based 순번(WAITING). 0 불가라 omitempty 안전
	Behind     int64  `json:"behind"`             // 내 뒤 인원(WAITING). 0도 유효값(맨 뒤) → omitempty 금지
	EtaSeconds int64  `json:"etaSeconds"`         // 예상 대기(초). -1=알수없음. 0도 유효값(곧 입장) → omitempty 금지
}

// position = GET /api/admission/position?movieId=&requestId=
// 3-state 판정(ZSCORE active→ADMITTED / ZRANK waiting→WAITING / 둘다없음→EXPIRED, §1-2).
func (a *Admission) position(c *gin.Context) {
	movieID := c.Query("movieId")
	requestID := c.Query("requestId")
	if movieID == "" || requestID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "movieId, requestId 필수"})
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
			resp.EtaSeconds = a.rate.ETASeconds(movieID, res.Position)
		}
	}
	c.JSON(http.StatusOK, resp)
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
	if err := a.rdb.Leave(c.Request.Context(), req.MovieID, req.RequestID); err != nil {
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
	removed, err := a.rdb.CompleteActive(c.Request.Context(), req.MovieID, req.RequestID)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "COMPLETED", "requestId": req.RequestID, "removed": removed})
}
