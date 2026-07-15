package processor

import (
	"context"
	"log/slog"
	"time"

	"cgv-onprem/queue-go/metrics"
	"cgv-onprem/queue-go/redis"
)

// QueueProcessor = 주기적으로 대기자를 active로 승격하는 백그라운드 루프(§1-3).
// [2-1] 로컬은 단일 인스턴스라 그냥 돎(멀티팟 리더선출은 2-2).
// AdmissionPublisher = 승격 시 입장 이벤트를 서비스간(Kafka)으로 발행. main이 구현 주입.
type AdmissionPublisher interface {
	PublishAdmission(ctx context.Context, requestID, movieID string) error
}

// promotePause = 발행 실패 후 승격을 쉬는 시간(설계서 1부 §3-A "승격 중단").
// 틱마다 승격→실패→롤백을 반복하는 churn을 막고, 지나면 자동 재시도 = "회복 시 재개".
const promotePause = 10 * time.Second

type QueueProcessor struct {
	rdb         *redis.Client
	maxSessions int64
	interval    time.Duration
	batchSize   int64
	publisher   AdmissionPublisher
	rate        *metrics.RateTracker
	pauseUntil  time.Time // 발행 실패 시 이 시각까지 승격 중단(루프 단일 고루틴만 접근 — 락 불필요)
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
	if time.Now().Before(p.pauseUntil) {
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
		// RPC 오류면 Lua는 서버에서 이미 승격을 끝냈을 수 있다 — 그 경우 승격자 목록을 잃어
		// 발행을 못 하고, 해당 유저는 booking 403 → 60s 타임아웃이 회수(알려진 한계로 수용,
		// 설계서 1부 §3-D ③ — 완전 방어는 결과 저널링이라 규모 대비 과함. 로그로 흔적만).
		slog.WarnContext(ctx, "processMovie 승격 실패(목록 유실 가능 — 60s 타임아웃이 회수)", "movie", movieID, "err", err)
		return
	}

	// 승격자마다 booking에게 입장 이벤트(Kafka admissions) 발행 — 안 하면 booking이 몰라 403.
	// (SSE ADMISSION 방송은 폐기 — 클라가 폴링으로 ADMITTED 발견, §1-3.)
	published := 0
	if p.publisher == nil {
		published = len(admitted) // 테스트 경로(발행자 미주입)
	} else {
		for i, requestID := range admitted {
			if err := p.publisher.PublishAdmission(ctx, requestID, movieID); err != nil {
				// (b) 정직한 실패(설계서 1부 §3-A): 발행 못 한 승격자(현재 포함 잔여)를
				// waiting 선두로 되돌리고(차례 보존) 승격을 잠시 중단.
				rollback := admitted[i:]
				if rbErr := p.rdb.RequeueFront(ctx, movieID, rollback, time.Now().UnixMilli()); rbErr != nil {
					// 롤백까지 실패(Redis도 이상) — 남은 방어는 60s 세션 타임아웃(자가치유).
					slog.ErrorContext(ctx, "승격 롤백 실패", "movie", movieID, "count", len(rollback), "err", rbErr)
				} else {
					slog.WarnContext(ctx, "발행 실패 → 승격자 waiting 선두로 롤백·승격 중단", "count", len(rollback), "pause", promotePause, "movie", movieID)
				}
				p.pauseUntil = time.Now().Add(promotePause)
				break
			}
			published++
		}
	}

	if published > 0 {
		slog.InfoContext(ctx, "승격·발행", "count", published, "movie", movieID)
	}

	// rate 관측(§1-3): 실제로 입장까지 완료(발행 성공)된 수만 반영 — 롤백분이 ETA를 부풀리지 않게.
	// 0명 관측도 의미 있음(rate 감소 → ETA 현실화).
	if p.rate != nil {
		p.rate.Observe(movieID, published, p.interval)
	}
}
