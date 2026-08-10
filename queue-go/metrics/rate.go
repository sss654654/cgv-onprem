// Package metrics = 승격 처리율(rate) 제공. ETA(예상 대기시간) 계산 재료.
// rate = 초당 승격 수. ETA = position ÷ rate.
//
// 값의 출처는 Redis 공유 버킷(redis.ObservePromotion / redis.PromotionRate)이다 —
// 파드마다 다른 값을 들고 있으면 같은 사용자가 폴링할 때마다 ETA가 갈리고(요청은 파드에
// 라운드로빈), 승격이 몰려 도는 주기 신호를 짧은 평활로 뭉개면 실제와 크게 어긋난다.
// 이 타입이 하는 일은 그 공유 값을 짧게 캐시하는 것뿐이다: position 폴링은 초당 요청이
// 가장 많은 경로라 매 요청이 창 전체를 읽으면 Redis 왕복이 요청 수에 비례해 늘어난다.
package metrics

import (
	"context"
	"sync"
	"time"

	"cgv-onprem/queue-go/redis"
)

// rateCacheTTL = 캐시 수명. 창(90초) 평균이라 1초 단위로는 거의 변하지 않는다.
const rateCacheTTL = 2 * time.Second

type rateEntry struct {
	val float64
	at  time.Time
}

// RateProvider = 영화별 승격 처리율 조회(공유 값 + 짧은 캐시).
type RateProvider struct {
	rdb *redis.Client
	mu  sync.Mutex
	c   map[string]rateEntry
}

func NewRateProvider(rdb *redis.Client) *RateProvider {
	return &RateProvider{rdb: rdb, c: make(map[string]rateEntry)}
}

// Rate = 영화의 초당 승격 수. 조회 실패나 기록 없음이면 0("아직 알 수 없음").
func (p *RateProvider) Rate(ctx context.Context, movieID string) float64 {
	now := time.Now()
	p.mu.Lock()
	if e, ok := p.c[movieID]; ok && now.Sub(e.at) < rateCacheTTL {
		p.mu.Unlock()
		return e.val
	}
	p.mu.Unlock()

	v, err := p.rdb.PromotionRate(ctx, movieID, now.Unix())
	if err != nil {
		// 조회 실패 시 마지막으로 알던 값을 그대로 쓴다 — 순단마다 ETA가 "계산 중"으로
		// 되돌아가지 않게. 알던 값이 없으면 0(=알 수 없음)이다.
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.c[movieID].val
	}
	p.mu.Lock()
	p.c[movieID] = rateEntry{val: v, at: now}
	p.mu.Unlock()
	return v
}

// ETASeconds = position ÷ rate. rate가 0에 가까우면 -1(알 수 없음) 반환.
func (p *RateProvider) ETASeconds(ctx context.Context, movieID string, position int64) int64 {
	r := p.Rate(ctx, movieID)
	if r < 0.001 || position <= 0 {
		return -1
	}
	return int64(float64(position) / r)
}
