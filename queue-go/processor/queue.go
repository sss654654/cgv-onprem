package processor

import (
	"context"
	"log/slog"
	"time"

	"cgv-onprem/queue-go/kafka"
	"cgv-onprem/queue-go/metrics"
	"cgv-onprem/queue-go/redis"
)

// QueueProcessor = 주기적으로 대기자를 active로 승격하는 백그라운드 루프(§1-3).
// AdmissionPublisher = 상태 변경을 서비스간(Kafka)으로 발행. main이 구현 주입.
// 승격·입장은 admissions로, 회수는 admissions-revoked로 나간다.
type AdmissionPublisher interface {
	PublishEvents(ctx context.Context, topic string, evs []kafka.Event) error
}

// promotePause = 발행 실패 후 승격을 쉬는 시간(설계서 1부 §3-A "승격 중단").
// 틱마다 승격→실패를 반복하는 churn을 막고, 지나면 자동 재개 = "회복 시 재개".
// 파드 로컬 변수가 아니라 Redis 키(TTL)로 둔다 — replica가 2 이상이면 로컬 변수로는
// 다른 파드가 계속 승격해 중단이 부분적으로만 걸린다.
const promotePause = 10 * time.Second

type QueueProcessor struct {
	rdb         *redis.Client
	maxSessions int64
	interval    time.Duration
	batchSize   int64
	publisher   AdmissionPublisher
	rate        *metrics.RateTracker
}

func NewQueueProcessor(rdb *redis.Client, maxSessions int64, interval time.Duration, batchSize int64, publisher AdmissionPublisher, rate *metrics.RateTracker) *QueueProcessor {
	return &QueueProcessor{rdb: rdb, maxSessions: maxSessions, interval: interval, batchSize: batchSize, publisher: publisher, rate: rate}
}

// Start = ctx가 취소될 때까지 interval마다 승격을 돈다. goroutine으로 띄워 쓴다.
func (p *QueueProcessor) Start(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	slog.InfoContext(ctx, "QueueProcessor started", "interval", p.interval, "maxSessions", p.maxSessions, "batch", p.batchSize)
	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "QueueProcessor stopped")
			return
		case <-ticker.C:
			p.processAll(ctx)
			metrics.LoopTick("promote") // 완주 도장(§7-B 행3) — 틱 끝에 찍어야 행(hang)이 잡힌다
		}
	}
}

func (p *QueueProcessor) processAll(ctx context.Context) {
	// 발행 실패 직후엔 승격 자체를 쉰다(§3-A) — 대기열 보존, Kafka 회복 후 재개.
	// 플래그가 Redis에 있어 전 파드가 같이 쉰다. 조회 실패는 승격을 막지 않는다(가용성 우선).
	if paused, err := p.rdb.PromotePaused(ctx); err == nil && paused {
		return
	}
	movies, err := p.rdb.ActiveQueueMovies(ctx)
	if err != nil {
		slog.WarnContext(ctx, "processAll: 영화 목록 조회 실패", "err", err)
		return
	}
	for _, movieID := range movies {
		p.processMovie(ctx, movieID)
	}
}

// processMovie = 한 영화의 빈자리만큼 대기자를 승격하고 admissions를 발행한다.
// vacant(빈자리) 계산은 promote Lua 안에서(P2, §1-3) → 여기선 maxSessions·batch만 넘김.
func (p *QueueProcessor) processMovie(ctx context.Context, movieID string) {
	waiting, err := p.rdb.WaitingCount(ctx, movieID)
	if err != nil {
		return
	}
	if waiting <= 0 {
		return // 대기자 없으면 패스(rate 관측도 불필요)
	}

	admitted, err := p.rdb.Promote(ctx, movieID, p.maxSessions, p.batchSize, time.Now().UnixMilli())
	if err != nil {
		// RPC 오류라도 Lua는 서버에서 이미 승격을 끝냈을 수 있다. 그 경우 승격자 명단은
		// 발행 대기 저널(pending_events)에 같은 원자 실행으로 남아 있으므로, 목록을 여기서
		// 잃어도 스윕 루프가 이어서 발행한다.
		slog.WarnContext(ctx, "processMovie 승격 실패(저널에 남았으면 스윕이 발행)", "movie", movieID, "err", err)
		return
	}
	if len(admitted) == 0 {
		if p.rate != nil {
			p.rate.Observe(movieID, 0, p.interval) // 0명도 관측(rate 감소 → ETA 현실화)
		}
		return
	}

	// 승격자에게 입장 이벤트(Kafka admissions)를 한 번에 발행 — 안 하면 booking이 몰라 403.
	// 건별 발행은 Writer 배치 타이머를 건마다 기다려 배치가 통째로 느려진다.
	published := len(admitted)
	if p.publisher != nil {
		evs := make([]kafka.Event, 0, len(admitted))
		members := make([]string, 0, len(admitted))
		for _, requestID := range admitted {
			evs = append(evs, kafka.Event{RequestID: requestID, MovieID: movieID})
			members = append(members, redis.PendingMember(redis.KindAdmit, redis.ReasonPromote, requestID))
		}
		if err := p.publisher.PublishEvents(ctx, kafka.TopicAdmissions, evs); err != nil {
			// 정직한 실패(§3-A): 저널을 지우지 않고 남긴다 → 스윕이 재발행한다.
			// 되돌리기(롤백)는 하지 않는다 — ctx 취소·타임아웃은 브로커 append 이후에도
			// 에러로 오므로, 되돌리면 booking엔 입장이 들어갔는데 queue만 자리를 뺏는 상태가 된다.
			// 승격은 잠시 멈춘다(전 파드 공통).
			if pErr := p.rdb.SetPromotePause(ctx, promotePause); pErr != nil {
				slog.ErrorContext(ctx, "승격 중단 플래그 설정 실패", "err", pErr)
			}
			slog.WarnContext(ctx, "발행 실패 → 저널 보존·승격 중단", "count", len(admitted), "pause", promotePause, "movie", movieID)
			published = 0
		} else if err := p.rdb.ClearPending(ctx, movieID, members); err != nil {
			// 발행은 됐는데 저널만 못 지웠다 — 스윕이 한 번 더 발행하지만
			// booking의 admissions 소비는 SADD라 멱등이므로 무해하다.
			slog.WarnContext(ctx, "저널 정리 실패(중복 발행 가능, 소비는 멱등)", "movie", movieID, "err", err)
		}
	}

	if published > 0 {
		slog.InfoContext(ctx, "승격·발행", "count", published, "movie", movieID)
	}

	// rate 관측(§1-3): 실제로 발행까지 끝난 수만 반영 — 실패분이 ETA를 부풀리지 않게.
	if p.rate != nil {
		p.rate.Observe(movieID, published, p.interval)
	}
}
