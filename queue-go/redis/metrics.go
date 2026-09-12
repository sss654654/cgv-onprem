package redis

import (
	"context"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// queue_redis_command_duration_seconds — 이 파드가 Redis 명령 하나에 쓴 시간.
//
// 왜 필요해졌나. 2026-09-12 stg 1만 명 판에서 순번 조회 p99 가 2.20초였는데 원인을 가릴 수
//   없었다. 그 판의 이 파드는 CPU 17% · 메모리 10% · 풀 대기 0회 · 풀 타임아웃 0회였다.
//   순번 조회는 Redis 명령 하나가 전부인 경로라 나머지 후보가 없는데, 그 명령에 몇 초가
//   걸렸는지를 재는 지표가 없었다.
// 앞서 이 계측을 넣지 않기로 한 근거는 "queue_redis_pool 계열과 redis_exporter 가 같은 판단을
//   준다" 였다. 그 근거가 stg 에서 성립하지 않는다 —
//   풀 지표는 연결을 얻기까지만 재고 명령이 도는 시간은 안 재며(그래서 위처럼 전부 0 이었다),
//   ElastiCache 에는 redis_exporter 를 붙이지 않는다. CloudWatch 가 주는 것은 서버 쪽
//   EngineCPU 이고, 클라이언트가 겪는 왕복 시간(네트워크 · TLS 포함)은 그 값에 안 들어간다.
//
// 이 파일이 metrics 패키지가 아니라 여기 있는 이유: metrics 가 이 패키지를 임포트한다
//   (StartSampler 가 *redis.Client 를 받는다). 반대로 임포트하면 순환이 된다.
//   promauto 는 기본 레지스트리에 등록하므로 노출 경로는 metrics 쪽과 같다.
//
// 라벨은 명령 이름 하나뿐이다. 이 서비스가 쓰는 명령은 정해져 있어(evalsha · exists · zadd ·
//   zrem · zrank · pipeline …) 시리즈가 그 수만큼만 는다. 인자는 담지 않는다 —
//   requestId 가 섞여 들어가면 시리즈가 사람 수만큼 늘고, 트레이싱에서 db.statement 를 끈
//   것과 같은 이유로 식별자가 저장소에 남는다.
//
// 버킷은 queue_http_request_duration_seconds 와 첫 칸을 맞춘다. Redis 명령이 정상일 때
//   0.1-1ms 라 기본 버킷(첫 칸 5ms)이면 분위수가 전부 첫 칸에 붙어 아무것도 못 가른다.
var commandDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name: "queue_redis_command_duration_seconds",
	Help: "Redis 명령 1회에 걸린 시간(클라이언트 기준 — 네트워크 · TLS 포함)",
	Buckets: []float64{
		0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01,
		0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5,
	},
}, []string{"command"})

// metricsHook = go-redis 훅. 명령이 도는 구간만 감싼다.
//
// DialHook 은 감싸지 않는다 — 연결 수립은 명령 시간이 아니라 풀의 일이고,
//   그 값은 queue_redis_pool 계열이 이미 준다. 여기 섞으면 첫 명령만 유독 느리게 보인다.
type metricsHook struct{}

func (metricsHook) DialHook(next goredis.DialHook) goredis.DialHook { return next }

func (metricsHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		start := time.Now()
		err := next(ctx, cmd)
		observe(cmd.Name(), time.Since(start))
		return err
	}
}

// 파이프라인은 묶음 하나를 한 번으로 센다. 안의 명령마다 세면 왕복 한 번이 여러 건으로
//   나뉘어, "Redis 왕복이 몇 번인가" 를 세던 판단이 어긋난다.
func (metricsHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		start := time.Now()
		err := next(ctx, cmds)
		observe("pipeline", time.Since(start))
		return err
	}
}

// 명령 이름은 소문자로 통일한다. go-redis 가 넘기는 이름의 대소문자가 경로마다 달라
//   같은 명령이 두 시리즈로 갈리는 것을 막는다.
func observe(name string, d time.Duration) {
	commandDuration.WithLabelValues(strings.ToLower(name)).Observe(d.Seconds())
}
