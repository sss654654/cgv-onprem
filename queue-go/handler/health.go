package handler

import (
	"net/http"
	"sync/atomic"

	"github.com/gin-gonic/gin"

	"cgv-onprem/queue-go/redis"
)

// Health는 헬스체크 핸들러 묶음. /ready에서 Redis를 PING하려고 클라이언트를 든다.
// 큐 서비스는 MySQL이 없어 readiness가 Redis만 본다. Kafka는 넣지 않는다 —
// 공유 의존성이라 넣으면 전 파드 동시 503 = 전면 중단(설계서 1부 §2-A).
// shuttingDown = graceful shutdown ①(설계서 1부 §2-C): SIGTERM 수신 시 main이 올리는
// 원자 플래그. ready가 503으로 바뀌어 Service 명단에서 빠지고 새 요청 유입이 끊긴다.
type Health struct {
	rdb          *redis.Client
	shuttingDown atomic.Bool
}

func NewHealth(rdb *redis.Client) *Health {
	return &Health{rdb: rdb}
}

// BeginShutdown은 readiness를 내린다(graceful ①). main의 SIGTERM 처리에서 호출.
func (h *Health) BeginShutdown() {
	h.shuttingDown.Store(true)
}

// Register는 헬스 라우트를 gin 엔진에 붙인다.
// (구 /health·/health/force-fail-503은 삭제 — 어디서도 안 쓰는 라우트와 상시 장애 스위치는
// "설명 못 하는 구성요소"라 제거. 드레인·장애 시연은 표준 수단으로 대체. 설계서 1부 §2-A.)
func (h *Health) Register(r *gin.Engine) {
	r.GET("/health/live", h.live)
	r.GET("/health/ready", h.ready)
}

// live = liveness probe. 프로세스가 살아있으면 UP. 실패 시 k8s가 재시작.
func (h *Health) live(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "UP"})
}

// ready = readiness probe. 종료 중이거나 Redis PING 실패면 503 → Service가
// 트래픽을 안 보냄(파드는 안 죽임). Redis failover 동안 잠깐 DOWN→자연복구.
func (h *Health) ready(c *gin.Context) {
	if h.shuttingDown.Load() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "DOWN", "reason": "SHUTTING_DOWN"})
		return
	}
	if err := h.rdb.Ping(c.Request.Context()); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "DOWN",
			"redis":  "DOWN",
			"error":  err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "UP", "redis": "UP"})
}
