package processor

import (
	"context"
	"log/slog"
	"time"

	"cgv-onprem/queue-go/kafka"
	"cgv-onprem/queue-go/metrics"
	"cgv-onprem/queue-go/redis"
)

// PendingSweeper = 발행 대기 저널(pending_events)에 남은 상태 변경을 Kafka로 내보낸다.
//
// 왜 저널이 필요한가: 상태 변경(승격·입장·타임아웃·이탈)은 Redis Lua에서 원자로 끝나지만
// Kafka 발행은 그 뒤의 별개 동작이다. 둘 사이에서 파드가 죽거나 발행이 실패하면
//   - 입장 통보가 안 나가 사용자는 화면상 입장했는데 booking은 몰라 계속 403,
//   - 회수 통보가 안 나가 자리를 잃은 사용자가 booking의 admitted에 남아 정원 밖에서 예매 가능.
// 저널은 상태 변경과 같은 원자 실행 안에서 기록되므로, 발행이 끝나야만 지워진다.
// 이 루프가 "지워지지 않고 남은 것"을 주기적으로 다시 내보내 두 구멍을 닫는다.
//
// 정상 경로도 저널에 쓰고 지운다. 그래서 방금 쓴 항목까지 스윕이 집으면 같은 이벤트가
// 두 번 나간다 — grace(유예)를 둬서 "발행 중일 수 있는" 최근 항목은 건너뛴다.
// 그래도 중복은 날 수 있는데, 소비 측이 SADD/SREM이라 멱등이므로 유실보다 중복을 택한다.
type PendingSweeper struct {
	rdb       *redis.Client
	publisher AdmissionPublisher
	interval  time.Duration
	grace     time.Duration
	batch     int64
}

// NewPendingSweeper — grace는 정상 발행이 끝날 시간보다 넉넉해야 한다(Writer 재시도 최악 약 5s).
func NewPendingSweeper(rdb *redis.Client, publisher AdmissionPublisher, interval, grace time.Duration, batch int64) *PendingSweeper {
	return &PendingSweeper{rdb: rdb, publisher: publisher, interval: interval, grace: grace, batch: batch}
}

func (s *PendingSweeper) Start(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	slog.InfoContext(ctx, "PendingSweeper started", "interval", s.interval, "grace", s.grace, "batch", s.batch)
	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "PendingSweeper stopped")
			return
		case <-ticker.C:
			s.processAll(ctx)
			metrics.LoopTick("pending_sweep") // 완주 도장 — 루프가 멈추면 이 지표가 늙는다
		}
	}
}

func (s *PendingSweeper) processAll(ctx context.Context) {
	if s.publisher == nil {
		return
	}
	movies, err := s.rdb.ActiveQueueMovies(ctx)
	if err != nil {
		slog.WarnContext(ctx, "sweep: 영화 목록 조회 실패", "err", err)
		return
	}
	cutoff := time.Now().Add(-s.grace).UnixMilli()
	for _, movieID := range movies {
		s.sweepMovie(ctx, movieID, cutoff)
	}
}

func (s *PendingSweeper) sweepMovie(ctx context.Context, movieID string, cutoff int64) {
	events, err := s.rdb.PendingBefore(ctx, movieID, cutoff, s.batch)
	if err != nil {
		slog.WarnContext(ctx, "sweep: 저널 조회 실패", "movie", movieID, "err", err)
		return
	}
	if len(events) == 0 {
		return
	}

	// 토픽별로 모아 한 번씩 발행한다(건별 호출은 배치 타이머를 건마다 기다린다).
	admits, revokes := []kafka.Event{}, []kafka.Event{}
	admitMembers, revokeMembers := []string{}, []string{}
	for _, e := range events {
		switch e.Kind {
		case redis.KindAdmit:
			admits = append(admits, kafka.Event{RequestID: e.RequestID, MovieID: movieID})
			admitMembers = append(admitMembers, e.Member)
		case redis.KindRevoke:
			revokes = append(revokes, kafka.Event{RequestID: e.RequestID, MovieID: movieID, Reason: e.Reason})
			revokeMembers = append(revokeMembers, e.Member)
		default:
			// 알 수 없는 kind는 재시도해도 같다 — 저널에 영원히 남지 않게 제거.
			slog.WarnContext(ctx, "sweep: 알 수 없는 이벤트 종류", "member", e.Member, "movie", movieID)
			_ = s.rdb.ClearPending(ctx, movieID, []string{e.Member})
		}
	}

	s.publishAndClear(ctx, movieID, kafka.TopicAdmissions, admits, admitMembers)
	s.publishAndClear(ctx, movieID, kafka.TopicRevoked, revokes, revokeMembers)
}

// publishAndClear = 발행에 성공한 경우에만 저널에서 지운다. 실패하면 남겨 다음 틱이 재시도한다.
func (s *PendingSweeper) publishAndClear(ctx context.Context, movieID, topic string, evs []kafka.Event, members []string) {
	if len(evs) == 0 {
		return
	}
	if err := s.publisher.PublishEvents(ctx, topic, evs); err != nil {
		slog.WarnContext(ctx, "sweep: 재발행 실패(저널 보존)", "topic", topic, "count", len(evs), "movie", movieID, "err", err)
		return
	}
	if err := s.rdb.ClearPending(ctx, movieID, members); err != nil {
		slog.WarnContext(ctx, "sweep: 저널 정리 실패(다음 틱 중복 발행 가능)", "movie", movieID, "err", err)
		return
	}
	slog.InfoContext(ctx, "sweep 재발행", "topic", topic, "count", len(evs), "movie", movieID)
}
