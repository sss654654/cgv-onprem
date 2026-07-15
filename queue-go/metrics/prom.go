// prom.go — promauto 계측(설계서 1부 §7-B "1차 7벌"). 노출은 별도 포트(METRICS_PORT,
// 기본 9091)의 /metrics — 인그레스·Service에 안 물리고 ServiceMonitor만 아는 포트(§5-D).
// 여기 심은 지표 + 공짜 지표(cAdvisor·kube-state·k6)가 §7-B 행1-행4를 채운다.
package metrics

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"cgv-onprem/queue-go/redis"
)

// ── 행1: 유저가 지금 겪는 것 ──────────────────────────────────────────
// RPS(count 증가율)·p99(분위수)·에러율(status 라벨)이 전부 이 하나에서 파생된다.
var httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "queue_http_request_duration_seconds",
	Help:    "HTTP 요청 처리시간 — 심판(RPS·p99·에러율 파생, 설계서 §7-B 행1)",
	Buckets: prometheus.DefBuckets,
}, []string{"path", "status"})

// GinMiddleware = 전 핸들러 공통 스톱워치 한 장(회사 promauto http의 재현).
// path 라벨은 라우트 패턴(c.FullPath())만 — 원시 URL을 라벨로 쓰면 카디널리티 폭발(§7-A 원칙 3).
func GinMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		path := c.FullPath()
		if path == "" {
			path = "unmatched" // 미등록 경로(404류)를 한 라벨로 뭉침 — 폭발 방지
		}
		httpDuration.WithLabelValues(path, strconv.Itoa(c.Writer.Status())).
			Observe(time.Since(start).Seconds())
	}
}

// ── 행2 · 대기열: 줄과 회전 ──────────────────────────────────────────────────
var (
	waitingGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queue_waiting", Help: "대기열 길이(설계서 §7-B 행2)"}, []string{"movie"})
	activeGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queue_active", Help: "입장(active) 인원 — 정원 소진율의 분자"}, []string{"movie"})
	rateGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queue_promotion_rate", Help: "초당 승격 수 — 유저 ETA에 쓰는 그 값(두 소비처 일치)"}, []string{"movie"})
)

// ── 행3: 겉으로 안 보이는 백그라운드 체인 ─────────────────────────────
var loopLastTick = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "queue_loop_last_tick_timestamp_seconds",
	Help: "루프가 마지막으로 완주한 시각 — time()−값이 자라면 stall(설계서 §7-B 행3)"},
	[]string{"loop"})

// LoopTick = 루프 한 바퀴 완주 도장. 틱 "끝"에 찍어야 완주가 증명된다(시작에 찍으면 행을 못 잡음).
// loop 라벨 값 = promote | session_timeout | waiting_timeout (코드 명칭 기준으로 통일).
func LoopTick(loop string) {
	loopLastTick.WithLabelValues(loop).SetToCurrentTime()
}

// RegisterFailureCounters = kafka 패키지의 atomic 카운터(단일 진실원)를 그대로 노출.
// 함수 주입으로 받는 이유: metrics→kafka import를 만들지 않기 위해(배선은 main이).
func RegisterFailureCounters(publish, consume func() float64) {
	promauto.NewCounterFunc(prometheus.CounterOpts{
		Name: "queue_kafka_publish_failures_total",
		Help: "admissions 발행 최종 실패(재시도 소진) 누적 — 전달이 새는 순간(설계서 §3-A)"}, publish)
	promauto.NewCounterFunc(prometheus.CounterOpts{
		Name: "queue_kafka_consume_failures_total",
		Help: "completed 처리 실패(미커밋) 누적 — 자리 반납 막힘(설계서 §3-B)"}, consume)
}

// ── 행4 · 자원: 심는 것은 풀뿐(CPU·스로틀·메모리는 cAdvisor 공짜) ─────────────
var poolGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "queue_redis_pool",
	Help: "Redis 커넥션 풀 — PoolSize 값의 근거이자 범인 '기다려서'(설계서 §7-B 행4)"},
	[]string{"state"})

// StartSampler = 상태 게이지 갱신 고루틴(파드당 1개, 유저 수 무관 — §7-B "심는 곳").
// 주기 5s = 폴링 최대 주기와 같은 급 — 상태 스냅샷은 그보다 자주 잴 이유가 없다.
//
// rate 배선: 지금은 유저 ETA가 쓰는 값(RateTracker EMA)을 그대로 노출한다 —
// "하나의 계측, 두 소비처" 일치 원칙. 멀티팟에서 파드별 선이 갈라져 보이는 것은
// §4-B가 진단한 문제로, [2-2] rate 재설계 때 이 소스만 promoted_count 샘플링으로
// 교체된다(그때 ETA와 지표가 함께 바뀜 — 일치 유지).
func StartSampler(ctx context.Context, rdb *redis.Client, rate *RateTracker, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sample(ctx, rdb, rate)
		}
	}
}

func sample(ctx context.Context, rdb *redis.Client, rate *RateTracker) {
	// 풀 장부 — go-redis PoolStats를 §7-B 라벨로 매핑:
	//   in_use = TotalConns − IdleConns / idle = IdleConns / waiting = PendingRequests(지금 커넥션 대기 중인 요청 수)
	// Total과 Idle은 별도 락으로 순차 샘플링돼 원자 스냅샷이 아니다 — 순간 역전 시 uint32
	//   언더플로(약 42억 스파이크)를 막기 위해 부호 있는 정수로 빼고 0 클램프.
	ps := rdb.PoolStats()
	inUse := int64(ps.TotalConns) - int64(ps.IdleConns)
	if inUse < 0 {
		inUse = 0
	}
	poolGauge.WithLabelValues("in_use").Set(float64(inUse))
	poolGauge.WithLabelValues("idle").Set(float64(ps.IdleConns))
	poolGauge.WithLabelValues("waiting").Set(float64(ps.PendingRequests))

	movies, err := rdb.ActiveQueueMovies(ctx)
	if err != nil {
		slog.WarnContext(ctx, "metrics sampler: 영화 목록 조회 실패", "err", err)
		return
	}
	for _, m := range movies {
		if n, err := rdb.WaitingCount(ctx, m); err == nil {
			waitingGauge.WithLabelValues(m).Set(float64(n))
		}
		if n, err := rdb.ActiveCount(ctx, m); err == nil {
			activeGauge.WithLabelValues(m).Set(float64(n))
		}
		rateGauge.WithLabelValues(m).Set(rate.Rate(m))
	}
}

// ServeMetrics = /metrics 전용 서버. 앱 포트(8090)와 분리 — 라우팅 실수로 지표가 샐
// 자리를 구조적으로 제거(§5-D 확정). 반환된 서버는 main의 graceful 경로에서 닫는다.
func ServeMetrics(port string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: ":" + port, Handler: mux, ReadHeaderTimeout: 3 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.ErrorContext(context.Background(), "metrics listen 실패", "err", err)
		}
	}()
	return srv
}
