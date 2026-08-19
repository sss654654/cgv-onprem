package redis

import (
	"context"
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

// New는 클라이언트를 만든다. 연결을 즉시 맺지 않고(lazy) 첫 명령 때 풀에서
// 꺼낸다 → 실제 도달은 Ping으로 확인한다. (옛 Java의 Lettuce 풀 = 여기 대응.)
//
// poolSize: 0이면 라이브러리 기본(10×GOMAXPROCS)을 쓴다 — automaxprocs 교정 후엔
// CPU limit 기준이라 안전하며, 실측값이 나오면 REDIS_POOL_SIZE로 명시한다.
// Read/WriteTimeout 500ms: 미설정 시 기본 약 3초 — "느려짐"이 그 3초 동안 고루틴·풀을
// 조용히 잠그는 꼬리가 되므로 수백 ms에서 fast fail. 실패는 에러로
// 드러나 관측 신호가 된다.
func New(addr, password string, poolSize int, masterName string, sentinelAddrs []string) *Client {
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
			DialTimeout:      2 * time.Second,
			ReadTimeout:      500 * time.Millisecond,
			WriteTimeout:     500 * time.Millisecond,
		}
		if poolSize > 0 {
			fo.PoolSize = poolSize
		}
		return &Client{rdb: instrument(goredis.NewFailoverClient(fo))}
	}
	opts := &goredis.Options{
		Addr:         addr,
		Password:     password,
		DB:           0,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  500 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond,
	}
	if poolSize > 0 {
		opts.PoolSize = poolSize
	}
	return &Client{rdb: instrument(goredis.NewClient(opts))}
}

// instrument = 명령마다 OTel span 을 붙인다. 호출부가 이미 ctx 를 끝까지 넘기고 있어
// (핸들러의 c.Request.Context() → redis 패키지 전 함수의 첫 인자) 훅 하나로 부모-자식이 이어진다.
//
// 이게 없으면 트레이스가 "HTTP 요청 하나" 에서 끝나, 느린 요청을 열어도 Redis 에서
// 기다린 것인지 그 밖인지 안 갈린다 — 지표는 총량만 주고 구간을 안 준다.
//
// 지표(metrics)는 켜지 않는다. 명령·상태별 카운터가 시리즈를 늘리는데, 같은 판단을
// queue_redis_pool 계열과 redis_exporter 가 이미 준다.
func instrument(rdb *goredis.Client) *goredis.Client {
	if err := redisotel.InstrumentTracing(rdb); err != nil {
		slog.Warn("redis 트레이싱 계측 실패(트레이스 없이 계속)", "err", err)
	}
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
