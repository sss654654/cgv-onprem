package redis

import (
	"context"
	"crypto/tls"
	"log/slog"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	goredis "github.com/redis/go-redis/v9"
)

// Client는 go-redis 클라이언트를 감싼 얇은 래퍼. 핸들러·서비스는 이 타입만
// 보고, go-redis(goredis) 의존은 이 패키지 안에 가둔다.
type Client struct {
	rdb *goredis.Client
}

// New는 클라이언트를 만든다. (옛 Java의 Lettuce 풀 = 여기 대응.)
//
// poolSize: 0이면 라이브러리 기본(10×GOMAXPROCS)을 쓴다 — automaxprocs 교정 후엔
// CPU limit 기준이라 안전하며, 실측값이 나오면 REDIS_POOL_SIZE로 명시한다.
//
// ★ 풀을 기동 때 채운다(MinIdleConns = PoolSize). 2026-09-12 stg 1만 명 판의 오픈 순간 실측:
//   고루틴 213 → 10,543 · 풀 대기(PendingRequests) 2,304 · 풀 사용 중 28 / 200 ·
//   ElastiCache 새 연결 1,675/분 · Redis 명령 273,472건 중 2.5초 초과 4,262건 ·
//   ElastiCache CPU 7.5% · 이 파드 CPU 0.18코어.
//   서버도 클라이언트도 한가한데 명령이 초 단위였다. 풀이 비어 있었기 때문이다 —
//   이 서비스는 오픈 전에 트래픽이 없어(로비 폴링은 frontend·booking 으로 간다) 파드가
//   몇 시간 떠 있어도 풀은 늘 빈 채로 오픈을 맞는다. go-redis 는 풀 자리를 쥔 채로 dial 하므로
//   첫 50개가 TLS 를 맺는 동안 그 자리를 아무도 못 쓰고, 나머지는 전부 기다린다.
//   미리 맺어 두면 오픈 순간 dial 이 0이 된다.
//
// ★ Read/WriteTimeout 500ms → 2s. 원래 근거는 "느려짐이 풀을 조용히 잠그는 꼬리가 되지 않게
//   fast fail" 이었는데, 버스트에서는 정반대로 작동했다. P 하나에 고루틴 수천 개가 몰리면
//   응답을 읽을 고루틴이 500ms 안에 차례를 못 받고, go-redis 는 타임아웃 난 커넥션을 버리고
//   재시도하면서 새로 dial 한다. 그래서 풀 상한이 200인데 새 연결이 분당 1,675개였다 —
//   fast fail 이 churn 을 만들어 풀을 잠갔다. 정상 명령은 수 ms 라 2초는 400배 여유이고,
//   진짜 장애는 여전히 초 단위로 드러난다.
// ★ MaxRetries 3 → 1. 타임아웃 2초 × 재시도 3회 = 6초 꼬리를 4초로 묶는다.
//   버스트에서는 재시도가 churn 의 재료이고, 장애에서는 fast fail 이 더 낫다.
//
// useTLS: 연결을 TLS 로 감싼다. 서버 인증서는 검증한다 — 검증을 끄면 같은 VPC 안에서
// 주소를 가로챈 쪽에 그대로 붙어 비밀번호를 넘겨주게 되고, 암호화를 켠 이유가 사라진다.
// ServerName 은 주소의 호스트에서 채워진다(crypto/tls DialWithDialer). ElastiCache 는
// 공인 인증서를 쓰므로 루트 인증서를 따로 넣지 않는다.
func New(addr, password string, poolSize int, masterName string, sentinelAddrs []string, useTLS bool) *Client {
	var tlsConf *tls.Config
	if useTLS {
		tlsConf = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	// sentinelAddrs가 있으면 Sentinel-aware(FailoverClient): master가 승격되면 Sentinel에 재조회해
	// 새 master로 재접속 → failover가 앱까지 반영된다(코드-반영 #3). 없으면 standalone(고정 Addr) =
	// 로컬 compose·dev 단일 인스턴스 경로. 타임아웃·풀은 양쪽 동일.
	if len(sentinelAddrs) > 0 {
		fo := &goredis.FailoverOptions{
			MasterName:       masterName,
			SentinelAddrs:    sentinelAddrs,
			Password:         password,
			SentinelPassword: password, // bitnami auth.sentinel(기본 on): sentinel도 같은 비번
			DB:               0,
			TLSConfig:        tlsConf,
			DialTimeout:      2 * time.Second,
			ReadTimeout:      2 * time.Second,
			WriteTimeout:     2 * time.Second,
			MaxRetries:       1,
		}
		if poolSize > 0 {
			fo.PoolSize = poolSize
			fo.MinIdleConns = poolSize
		}
		return &Client{rdb: instrument(goredis.NewFailoverClient(fo))}
	}
	opts := &goredis.Options{
		Addr:         addr,
		Password:     password,
		DB:           0,
		TLSConfig:    tlsConf,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		MaxRetries:   1,
	}
	if poolSize > 0 {
		opts.PoolSize = poolSize
		opts.MinIdleConns = poolSize
	}
	return &Client{rdb: instrument(goredis.NewClient(opts))}
}

// instrument = 명령마다 OTel span 을 붙인다. 호출부가 이미 ctx 를 끝까지 넘기고 있어
// (핸들러의 c.Request.Context() → redis 패키지 전 함수의 첫 인자) 훅 하나로 부모-자식이 이어진다.
//
// 이게 없으면 트레이스가 "HTTP 요청 하나" 에서 끝나, 느린 요청을 열어도 Redis 에서
// 기다린 것인지 그 밖인지 안 갈린다 — 지표는 총량만 주고 구간을 안 준다.
//
// redisotel 의 지표(InstrumentMetrics)는 켜지 않는다 — 명령·상태별 카운터가 시리즈를 늘린다.
// 대신 명령에 걸린 시간만 재는 훅을 직접 단다(metrics.go 의 queue_redis_command_duration_seconds).
// 앞서 이 계측을 통째로 안 넣은 근거는 "queue_redis_pool 계열과 redis_exporter 가 같은 판단을
//   준다" 였는데, stg 에서 둘 다 성립하지 않았다. 풀 지표는 연결을 얻기까지만 재고(1만 명 판에서
//   대기 0회 · 타임아웃 0회였는데 순번 조회 p99 는 2.20초였다), ElastiCache 에는
//   redis_exporter 를 붙이지 않는다.
//
// WithDBStatement(false) — 기본값이 true 라 명령 인자가 db.statement 에 통째로 실린다.
// Tempo 에 저장된 실제 span 에서 확인한 값:
//   evalsha <sha> 6 sessions:{1}:active ... 500 <requestId> <ms> 1
//   zrem pending_events:{1} A|ENTER|<requestId>
// requestId 는 클라이언트가 만들어 보관하는 대기열 식별자다. 끄지 않으면 그 값이
// 트레이스 저장소에 남고, 트레이스를 조회할 수 있는 사람은 누구나 읽는다.
// 끄면 db.statement 가 명령 이름만 남는다 — 어느 명령에서 느렸는지는 그대로 보이므로
// 이 계측을 넣은 목적(요청 안에서 Redis 구간을 가르는 것)은 유지된다.
func instrument(rdb *goredis.Client) *goredis.Client {
	if err := redisotel.InstrumentTracing(rdb, redisotel.WithDBStatement(false)); err != nil {
		slog.Warn("redis 트레이싱 계측 실패(트레이스 없이 계속)", "err", err)
	}
	rdb.AddHook(metricsHook{})
	return rdb
}

// Ping은 Redis 도달 여부를 확인한다(헬스체크 /health/ready용).
// 실효 상한은 클라이언트 Read/WriteTimeout(500ms)이 지배한다 — 여기 2s ctx는
// 그보다 넓은 상한일 뿐(go-redis는 둘 중 이른 데드라인을 택함).
func (c *Client) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return c.rdb.Ping(ctx).Err()
}

// PoolStats는 go-redis 풀 내부 장부를 그대로 통과시킨다 — queue_redis_pool 게이지의
// 유일한 재료. 래퍼가 rdb를 감추고 있어 이 통과 메서드가 노출 창구다.
func (c *Client) PoolStats() *goredis.PoolStats {
	return c.rdb.PoolStats()
}

// Close는 연결 풀을 닫는다(graceful shutdown ④ — 의존의 역순, 맨 마지막).
func (c *Client) Close() error {
	return c.rdb.Close()
}
