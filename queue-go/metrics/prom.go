// prom.go — promauto 계측. 노출은 별도 포트(METRICS_PORT,
// 기본 9091)의 /metrics — 인그레스·Service에 안 물리고 ServiceMonitor만 아는 포트.
// 여기 심은 지표 + 공짜 지표(cAdvisor·kube-state·k6)가 서비스 대시보드 행1-행4를 채운다.
package metrics

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/pprof"
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
	Help:    "HTTP 요청 처리시간 — 심판(RPS·p99·에러율 파생)",
	Buckets: prometheus.DefBuckets,
}, []string{"path", "status"})

// GinMiddleware = 전 핸들러 공통 스톱워치 한 장(회사 promauto http의 재현).
// path 라벨은 라우트 패턴(c.FullPath())만 — 원시 URL을 라벨로 쓰면 카디널리티 폭발.
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
		Name: "queue_waiting", Help: "대기열 길이"}, []string{"movie"})
	activeGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queue_active", Help: "입장(active) 인원 — 정원 소진율의 분자"}, []string{"movie"})
	rateGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queue_promotion_rate", Help: "초당 승격 수 — 유저 ETA에 쓰는 그 값(두 소비처 일치)"}, []string{"movie"})
)

// ── 행3: 겉으로 안 보이는 백그라운드 체인 ─────────────────────────────
var loopLastTick = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "queue_loop_last_tick_timestamp_seconds",
	Help: "루프가 마지막으로 완주한 시각 — time()−값이 자라면 stall"},
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
		Help: "admissions 발행 최종 실패(재시도 소진) 누적 — 전달이 새는 순간"}, publish)
	promauto.NewCounterFunc(prometheus.CounterOpts{
		Name: "queue_kafka_consume_failures_total",
		Help: "completed 처리 실패(미커밋) 누적 — 자리 반납 막힘"}, consume)
}

// ── 행4 · 자원: 심는 것은 풀뿐(CPU·스로틀·메모리는 cAdvisor 공짜) ─────────────
var poolGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "queue_redis_pool",
	Help: "Redis 커넥션 풀 — PoolSize 값의 근거이자 범인 '기다려서'"},
	[]string{"state"})

// StartSampler = 상태 게이지 갱신 고루틴(파드당 1개, 유저 수 무관).
// 주기 5s = 폴링 최대 주기와 같은 급 — 상태 스냅샷은 그보다 자주 잴 이유가 없다.
//
// rate 배선: 유저 ETA가 쓰는 값과 같은 소스(Redis 공유 버킷의 창 평균)를 노출한다 —
// "하나의 계측, 두 소비처" 일치 원칙. 값이 공유라 파드가 여러 개여도 선이 갈라지지 않는다.
func StartSampler(ctx context.Context, rdb *redis.Client, rate *RateProvider, interval time.Duration) {
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

func sample(ctx context.Context, rdb *redis.Client, rate *RateProvider) {
	// 풀 장부 — go-redis PoolStats를 대시보드 라벨로 매핑:
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
	cur := make(map[string]struct{}, len(movies))
	for _, m := range movies {
		cur[m] = struct{}{}
		if n, err := rdb.WaitingCount(ctx, m); err == nil {
			waitingGauge.WithLabelValues(m).Set(float64(n))
		}
		if n, err := rdb.ActiveCount(ctx, m); err == nil {
			activeGauge.WithLabelValues(m).Set(float64(n))
		}
		rateGauge.WithLabelValues(m).Set(rate.Rate(ctx, m))
	}

	// 추적 목록에서 빠진 영화의 게이지를 지운다.
	// 안 지우면 마지막 값이 그대로 남아, 줄이 빈 뒤에도 화면이 "대기 402명"을 계속 보여준다
	// (게이지는 갱신하지 않으면 직전 값을 유지한다). 0으로 내리는 방법도 있으나 그러면
	// 지나간 영화만큼 시리즈가 영구히 쌓여, 추적 해제로 카디널리티를 줄이려던 목적이 무너진다.
	// 시리즈가 사라져 화면이 비는 것은 대시보드 쿼리의 `or vector(0)`이 받는다.
	for m := range trackedMovies {
		if _, ok := cur[m]; ok {
			continue
		}
		waitingGauge.DeleteLabelValues(m)
		activeGauge.DeleteLabelValues(m)
		rateGauge.DeleteLabelValues(m)
	}
	trackedMovies = cur
}

// trackedMovies = 직전 tick에서 추적 중이던 영화. sample()은 단일 고루틴에서만
// 호출되므로(StartSampler의 ticker 루프) 별도 동기화가 필요 없다.
var trackedMovies = map[string]struct{}{}

// ServeMetrics = /metrics 전용 서버. 앱 포트(8090)와 분리 — 라우팅 실수로 지표가 샐
// 자리를 구조적으로 제거. 반환된 서버는 main의 graceful 경로에서 닫는다.
//
// /debug/pprof 도 이 포트에만 연다. 지표는 총량만 알려주고 그 안에 무엇이 들었는지는
// 말하지 않는다 — 힙이 26MiB 라는 것은 알아도 그중 무엇이 얼마인지는 pprof 로만 본다.
// 앱 포트에 열면 인그레스를 통해 밖에서 스택·힙 상태에 닿을 수 있으므로 여기에만 둔다.
func ServeMetrics(port string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	srv := &http.Server{Addr: ":" + port, Handler: mux, ReadHeaderTimeout: 3 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.ErrorContext(context.Background(), "metrics listen 실패", "err", err)
		}
	}()
	return srv
}
