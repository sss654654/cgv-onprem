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
// 버킷: 기본값(DefBuckets)의 첫 칸이 5ms인데 폴링 응답이 2-5ms라 분위수가 전부 첫 칸
//   안에 들어갔다. 그러면 histogram_quantile이 0과 0.005 사이를 선형보간해 p50 0.0025 ·
//   p99 0.005를 상수로 내놓는다 — 실측(2026-08-13·08-14 부하 판) 전 구간이 그 값이었다.
//   앞쪽에 0.001·0.0025를 넣어 그 구간을 갈랐다. 뒤쪽은 기본값 그대로 10초까지.
// 시리즈 비용: 경로 7 × status 종류 × 버킷 수. 버킷이 11 → 13이라 경로당 2 시리즈씩 는다.
var httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name: "queue_http_request_duration_seconds",
	Help: "HTTP 요청 처리시간 — 심판(RPS·p99·에러율 파생)",
	Buckets: []float64{
		0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
		0.1, 0.25, 0.5, 1, 2.5, 5, 10,
	},
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

	// 누적 승격 수. 위 세 값은 순간값이라 "이 구간에 몇 명이 줄을 통과했나"를 못 센다.
	// 승격은 HTTP 요청이 아니라 배경 루프가 하므로 요청 지표로도 셀 수 없다.
	// rate 게이지를 시간으로 적분하는 방법은 스크레이프 간격에 따라 값이 달라져 못 쓴다.
	promotedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "queue_promoted_total", Help: "승격 누적 인원 — 구간 통과 인원은 increase()로 낸다"},
		[]string{"movie"})

	capacityGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "queue_capacity", Help: "입장 정원(MAX_SESSIONS) — 소진율의 분모"})
)

// Promoted = 승격이 실제로 일어난 인원수를 더한다. Lua가 승격을 끝내고 명단을 돌려준 뒤 호출한다.
func Promoted(movieID string, n int) {
	if n > 0 {
		promotedTotal.WithLabelValues(movieID).Add(float64(n))
	}
}

// SetCapacity — 기동 시 한 번. 설정값이라 도중에 바뀌지 않는다.
// 지표로 내는 이유: 정원은 대시보드에서 소진율의 분모이자 계단 판의 축인데, 상수로 적어 두면
// 계단을 올릴 때마다 화면을 같이 고쳐야 하고 안 고치면 앞 계단 기준으로 읽힌다.
func SetCapacity(n int64) { capacityGauge.Set(float64(n)) }

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

// ── Kafka 왕복: booking 이 예매를 확정한 시각부터 자리가 실제로 빈 시각까지 ─────
//
// 회전 속도(정원 ÷ 체류 시간)에 이 구간이 통째로 들어간다. 발행 건수와 소비 건수는 각각
//   지표로 나오지만 둘이 같아도 이 구간이 늘어났는지는 알 수 없다 — 한 건이 건너오는 데
//   걸린 시간은 어느 지표에도 없다.
// 경계는 queue_http_request_duration_seconds 와 같은 눈금(뒤쪽 절반)이다.
var completedLag = promauto.NewHistogram(prometheus.HistogramOpts{
	Name: "queue_completed_lag_seconds",
	Help: "예매 확정 발행(booking) → 자리 반환 처리(queue) 지연",
	Buckets: []float64{
		0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
		0.1, 0.25, 0.5, 1, 2.5, 5, 10,
	},
})

// ObserveCompletedLag — 메시지의 발행 시각과 지금의 차. 발행 시각은 카프카 레코드
// 타임스탬프(기본 CreateTime)라 booking 파드의 시계로 찍힌다 — 두 노드의 시계 차이가
// 그대로 오차로 섞인다. 시계가 역전돼 음수가 나오면 지연이 아니라 잡음이므로 버린다.
func ObserveCompletedLag(d time.Duration) {
	if d >= 0 {
		completedLag.Observe(d.Seconds())
	}
}

// ── 행4 · 자원: 심는 것은 풀뿐(CPU·스로틀·메모리는 cAdvisor 공짜) ─────────────
var poolGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "queue_redis_pool",
	Help: "Redis 커넥션 풀 — PoolSize 값의 근거이자 범인 '기다려서'"},
	[]string{"state"})

// RegisterPoolCounters는 풀 누적 카운터 셋을 등록한다. main에서 한 번 부른다.
//
// 위 게이지는 5초마다 찍는 순간값이라 왕복 1ms짜리 대기를 못 잡는다 — 실측(2026-08-14,
//   대기 998명)에서 in_use와 waiting이 전 구간 0이었다. "안 기다렸다"가 아니라 못 본 것이다.
// go-redis가 이미 세고 있는 누적값을 그대로 내보낸다. 누적이라 샘플 사이에 일어난 사건도
//   남으므로, 풀이 좁은지는 이 셋으로만 판정된다.
// CounterFunc로 두면 스크레이프 시점에 읽어 샘플러 주기와 무관하게 최신값이 나온다.
func RegisterPoolCounters(rdb *redis.Client) {
	promauto.NewCounterFunc(prometheus.CounterOpts{
		Name: "queue_redis_pool_wait_count_total",
		Help: "커넥션을 기다린 요청 누적 건수 — 0이 아니면 풀이 한 번이라도 말랐다",
	}, func() float64 { return float64(rdb.PoolStats().WaitCount) })

	promauto.NewCounterFunc(prometheus.CounterOpts{
		Name: "queue_redis_pool_wait_seconds_total",
		Help: "커넥션을 기다린 시간 누적(초) — wait_count로 나누면 평균 대기 시간",
	}, func() float64 { return float64(rdb.PoolStats().WaitDurationNs) / 1e9 })

	promauto.NewCounterFunc(prometheus.CounterOpts{
		Name: "queue_redis_pool_timeouts_total",
		Help: "풀 대기가 타임아웃으로 끝난 누적 건수 — 0이 아니면 풀 크기가 상한이다",
	}, func() float64 { return float64(rdb.PoolStats().Timeouts) })
}

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
