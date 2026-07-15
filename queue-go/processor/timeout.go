package processor

import (
	"context"
	"log/slog"
	"time"

	"cgv-onprem/queue-go/metrics"
	"cgv-onprem/queue-go/redis"
)

// SessionTimeoutProcessor = active 세션 중 timeout 초과한 것을 주기적으로 퇴장시킨다(§1-4a).
// score(입장시각) 기준 만료 판정. 좌석락(seat:)은 Redis EX 자동만료지만,
// active ZSet은 멤버별 TTL이 안 돼 이렇게 수동으로 쓸어낸다.
// 제거된 사람은 다음 폴링/좌석요청에서 발견(SSE TIMEOUT 방송은 폐기, §1-4a).
type SessionTimeoutProcessor struct {
	rdb      *redis.Client
	timeout  time.Duration
	interval time.Duration
}

func NewSessionTimeoutProcessor(rdb *redis.Client, timeout, interval time.Duration) *SessionTimeoutProcessor {
	return &SessionTimeoutProcessor{rdb: rdb, timeout: timeout, interval: interval}
}

func (p *SessionTimeoutProcessor) Start(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	slog.InfoContext(ctx, "SessionTimeoutProcessor started", "timeout", p.timeout, "interval", p.interval)
	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "SessionTimeoutProcessor stopped")
			return
		case <-ticker.C:
			p.processAll(ctx)
			metrics.LoopTick("session_timeout") // 완주 도장(§7-B 행3)
		}
	}
}

func (p *SessionTimeoutProcessor) processAll(ctx context.Context) {
	movies, err := p.rdb.ActiveQueueMovies(ctx)
	if err != nil {
		slog.WarnContext(ctx, "timeout processAll: 영화 목록 조회 실패", "err", err)
		return
	}
	// cutoff = now - timeout. score(입장시각)가 이보다 작거나 같으면 만료.
	cutoff := time.Now().Add(-p.timeout).UnixMilli()
	for _, movieID := range movies {
		expired, err := p.rdb.ExpireActive(ctx, movieID, cutoff)
		if err != nil {
			slog.WarnContext(ctx, "active 타임아웃 처리 실패", "movie", movieID, "err", err)
			continue
		}
		if len(expired) > 0 {
			slog.InfoContext(ctx, "active 타임아웃", "count", len(expired), "movie", movieID, "expired", expired)
		}
	}
}
