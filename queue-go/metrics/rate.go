// Package metrics = 승격 처리율(rate) 추적. ETA(예상 대기시간) 계산 재료.
// 백엔드서비스-올인원.md §1-3: rate = 초당 승격 수. ETA = position ÷ rate.
// [2-1] 로컬은 단일 인스턴스라 in-memory로 충분(리더선출·Redis 백업은 2-2).
package metrics

import (
	"sync"
	"time"
)

// RateTracker = 영화별 "초당 승격 수"를 EMA(지수이동평균)로 추적.
//   - 승격 루프가 매 틱 Observe(승격수) → 순간율을 EMA로 완만하게.
//   - position 핸들러가 Rate()로 읽어 ETA 계산.
// EMA를 쓰는 이유(§1-3): 승격은 2초 루프라 순간값이 요동(0이었다 튀었다) →
//   EMA로 완만한 rate. 승격이 멈추면 rate가 0으로 수렴 → ETA "알 수 없음"(rate=0 가드).
type RateTracker struct {
	mu    sync.RWMutex
	rate  map[string]float64 // movieID → 초당 승격 수(EMA)
	alpha float64            // EMA 가중치(최신 관측 비중)
}

func NewRateTracker() *RateTracker {
	return &RateTracker{rate: make(map[string]float64), alpha: 0.3}
}

// Observe = 한 틱에서 promoted명 승격됨(interval 동안). 순간율을 EMA에 반영.
// promoted=0(자리 없어 못 올림)도 반영해야 rate가 현실적으로 감소한다.
func (t *RateTracker) Observe(movieID string, promoted int, interval time.Duration) {
	sec := interval.Seconds()
	if sec <= 0 {
		return
	}
	inst := float64(promoted) / sec
	t.mu.Lock()
	t.rate[movieID] = t.alpha*inst + (1-t.alpha)*t.rate[movieID]
	t.mu.Unlock()
}

// Rate = 영화의 현재 초당 승격 수(EMA). 0이면 "아직 알 수 없음".
func (t *RateTracker) Rate(movieID string) float64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.rate[movieID]
}

// ETASeconds = position ÷ rate. rate가 0에 가까우면 -1(알 수 없음) 반환(§1-3 rate=0 가드).
func (t *RateTracker) ETASeconds(movieID string, position int64) int64 {
	r := t.Rate(movieID)
	if r < 0.001 || position <= 0 {
		return -1
	}
	return int64(float64(position) / r)
}
