package processor

import (
	"context"
	"log/slog"
	"time"

	"cgv-onprem/queue-go/metrics"
	"cgv-onprem/queue-go/redis"
)

// WaitingTimeoutProcessor = 폴링이 끊긴 대기자를 주기적으로 evict한다(§1-4b).
// SSE 연결끊김 감지의 폴링판 — "마지막 폴링(lastseen)이 오래된 사람 = 나간 것"으로 유추.
// waiting·waiting_lastseen 둘 다에서 원자 제거(ExpireWaiting, §1-3 정합 규칙).
// [2-1] 로컬은 단일 인스턴스라 그냥 돎(멀티팟 리더선출은 2-2).
type WaitingTimeoutProcessor struct {
	rdb      *redis.Client
	timeout  time.Duration // 이 시간 이상 폴링 없으면 evict(폴링 주기보다 넉넉히)
	interval time.Duration // 검사 주기
}

func NewWaitingTimeoutProcessor(rdb *redis.Client, timeout, interval time.Duration) *WaitingTimeoutProcessor {
	return &WaitingTimeoutProcessor{rdb: rdb, timeout: timeout, interval: interval}
}

func (p *WaitingTimeoutProcessor) Start(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	slog.InfoContext(ctx, "WaitingTimeoutProcessor started", "timeout", p.timeout, "interval", p.interval)
	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "WaitingTimeoutProcessor stopped")
			return
		case <-ticker.C:
			p.processAll(ctx)
			metrics.LoopTick("waiting_timeout") // 완주 도장(§7-B 행3)
		}
	}
}

func (p *WaitingTimeoutProcessor) processAll(ctx context.Context) {
	movies, err := p.rdb.ActiveQueueMovies(ctx)
	if err != nil {
		slog.WarnContext(ctx, "waiting timeout processAll: 영화 목록 조회 실패", "err", err)
		return
	}
	// cutoff = now - timeout. lastseen(마지막 폴링)이 이보다 오래되면 evict.
	cutoff := time.Now().Add(-p.timeout).UnixMilli()
	for _, movieID := range movies {
		evicted, err := p.rdb.ExpireWaiting(ctx, movieID, cutoff)
		if err != nil {
			slog.WarnContext(ctx, "waiting 타임아웃 처리 실패", "movie", movieID, "err", err)
			continue
		}
		if len(evicted) > 0 {
			slog.InfoContext(ctx, "waiting 폴링 타임아웃", "count", len(evicted), "movie", movieID, "evicted", evicted)
		}
	}
}
