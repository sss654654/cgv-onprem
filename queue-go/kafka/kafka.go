package kafka

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"

	"cgv-onprem/queue-go/metrics"
	"cgv-onprem/queue-go/redis"
)

// kafkaHeaderCarrier = OTel TextMapCarrier를 kafka-go 메시지 헤더에 어댑트.
// W3C traceparent를 헤더로 주고받아 producer(booking·queue)→consumer trace를 잇는다.
type kafkaHeaderCarrier []kafkago.Header

func (c *kafkaHeaderCarrier) Get(key string) string {
	for _, h := range *c {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *kafkaHeaderCarrier) Set(key, value string) {
	for i := range *c {
		if (*c)[i].Key == key {
			(*c)[i].Value = []byte(value)
			return
		}
	}
	*c = append(*c, kafkago.Header{Key: key, Value: []byte(value)})
}

func (c *kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, len(*c))
	for i, h := range *c {
		keys[i] = h.Key
	}
	return keys
}

const (
	TopicAdmissions = "admissions"         // queue → booking (입장 승인)
	TopicRevoked    = "admissions-revoked" // queue → booking (입장 회수: 세션 만료·이탈·자리반환)
	TopicCompleted  = "bookings-completed" // booking → queue (자리 반환)
)

type Event struct {
	RequestID string `json:"requestId"`
	MovieID   string `json:"movieId"`
	// Reason은 회수 이벤트에만 채운다(SESSION_TIMEOUT·LEAVE·COMPLETE). 입장 이벤트에선 빈 값.
	// 소비 측은 이 필드가 없어도 동작해야 한다(omitempty — 기존 admissions 스키마와 호환).
	Reason string `json:"reason,omitempty"`
}

// PublishFailures = 발행 최종 실패(내부 재시도 소진 후) 누적. ConsumeFailures = completed
// 처리 실패(미커밋) 누적. 단일 진실원은 이 atomic이고, promauto CounterFunc가 이 값을 그대로
// 읽어 queue_kafka_publish/consume_failures_total로 노출한다.
var (
	PublishFailures atomic.Int64
	ConsumeFailures atomic.Int64
)

// Producer = "admissions" 발행. 승격 시 호출 → booking이 admitted set을 채운다.
type Producer struct{ w *kafkago.Writer }

// NewProducer는 즉시 반환한다(기동 비동기화).
// 토픽 보장(ensureTopic)은 백그라운드로 뺐다: Kafka가 늦게 떠도 HTTP 포트는 열린다.
// 첫 발행은 AllowAutoTopicCreation + Writer 내부 재시도가 받치고, 그래도 실패하면
// 발행실패 처리가 받는다. ctx = main의 루트 ctx — 종료 시 대기 중단.
func NewProducer(ctx context.Context, broker string) *Producer {
	go ensureTopic(ctx, broker, TopicAdmissions)
	go ensureTopic(ctx, broker, TopicRevoked)
	return &Producer{w: &kafkago.Writer{
		Addr: kafkago.TCP(broker),
		// Topic을 Writer에 고정하지 않는다 — 입장(admissions)과 회수(admissions-revoked)를
		// 같은 Writer로 보내려면 메시지마다 Topic을 지정해야 한다.
		//
		// Balancer=Hash + Key=requestId: 같은 사용자의 이벤트는 한 토픽 안에서 항상 같은
		// 파티션으로 간다. 사용자 간에는 순서 의존이 없어 파티션을 고르게 쓰면서 필요한 순서를 지킨다.
		//
		// ★ 이 라우팅이 지키는 것은 한 토픽 안의 순서다. 입장(admissions)과 회수(admissions-revoked)는
		// 서로 다른 토픽이고 소비자도 각각 따로 돌아, 둘 사이의 처리 순서는 보장되지 않는다.
		// 회수가 먼저 처리되면 뒤늦게 도착한 입장이 이미 자리를 잃은 사용자의 인증을 되살린다.
		// 성립하려면 입장 소비가 세션 타임아웃(기본 60초)보다 오래 밀려야 한다 — 그 정도 지연은
		// 이미 장애 구간이라 지금은 감수한다. 닫으려면 두 이벤트를 한 토픽으로 합치거나
		// 소비 쪽에서 발행 시각을 비교해 오래된 입장을 버려야 한다.
		Balancer:               &kafkago.Hash{},
		AllowAutoTopicCreation: true,
		// RequiredAcks 기본값(RequireNone)은 브로커 확인 없이 성공 반환 — "발행 실패 처리"가
		// 성립하려면 실패를 알아야 하므로 ack 필수.
		RequiredAcks: kafkago.RequireOne,
		MaxAttempts:  3, // 짧은 재시도 — 몇 초 순단은 여기서 흡수
		// WriteTimeout은 "시도당" 상한이다 — kafka-go는 시도마다 새 타임아웃을 만들고
		// 재시도 사이에 백오프를 둔다. 최악 지연 ≈ 1s×3 + 백오프 = 약 5s로,
		// enter 경로의 서버 WriteTimeout(10s) 안쪽으로 묶인다.
		WriteTimeout: time.Second,
		// BatchTimeout 기본값은 1초다. 승격 루프가 1건씩 동기로 WriteMessages를 부르면
		// 건마다 이 1초를 기다려 100명 승격에 약 100초가 걸린다. 배치 발행(PublishEvents)에
		// 여러 건을 한 번에 넘기더라도, 마지막 부분 배치를 오래 붙들지 않도록 짧게 잡는다.
		BatchTimeout: 10 * time.Millisecond,
	}}
}

// PublishAdmission — processor·handler의 AdmissionPublisher 인터페이스를 만족.
// Writer 내부 재시도(MaxAttempts=3) 후에도 실패면 에러를 돌려준다. 그다음은 호출자의
// 정직한 실패: enter=보상 롤백+재시도 안내, 승격 루프=롤백+승격 일시 중단.
func (p *Producer) PublishAdmission(ctx context.Context, requestID, movieID string) error {
	return p.PublishEvents(ctx, TopicAdmissions, []Event{{RequestID: requestID, MovieID: movieID}})
}

// PublishRevoke = 입장 회수 통보. booking이 자기 admitted에서 그 사용자를 지운다.
// reason은 SESSION_TIMEOUT·LEAVE·COMPLETE 중 하나(관측·디버깅용 — 소비 로직은 구분하지 않는다).
func (p *Producer) PublishRevoke(ctx context.Context, requestID, movieID, reason string) error {
	return p.PublishEvents(ctx, TopicRevoked, []Event{{RequestID: requestID, MovieID: movieID, Reason: reason}})
}

// PublishEvents = 같은 토픽으로 여러 이벤트를 한 번의 WriteMessages 호출로 보낸다.
// 건별 호출은 메시지마다 Writer의 배치 타이머를 기다려 승격 배치가 통째로 느려진다.
// 부분 실패는 없다 — WriteMessages는 전부 성공하거나 에러를 돌려준다. 호출자는 실패 시
// 발행 대기 저널을 지우지 않고 남겨 다음 스윕이 재발행하게 한다.
func (p *Producer) PublishEvents(ctx context.Context, topic string, evs []Event) error {
	if len(evs) == 0 {
		return nil
	}
	// trace: 발행 span을 열고 W3C traceparent를 Kafka 헤더에 inject.
	//   enter 경로는 ctx에 HTTP 서버 span이 있어 그 자식으로, 승격 루프는 background(span 없음)라
	//   여기서 뜨는 span이 새 루트가 된다 — 두 발행 지점 모두 유효한 traceparent를 싣는다.
	ctx, span := otel.Tracer("queue-kafka").Start(ctx, "publish "+topic)
	defer span.End()

	var carrier kafkaHeaderCarrier
	otel.GetTextMapPropagator().Inject(ctx, &carrier)

	msgs := make([]kafkago.Message, 0, len(evs))
	for _, e := range evs {
		b, err := json.Marshal(e)
		if err != nil {
			// 직렬화가 실패하는 값은 재시도해도 같다 — 배치 전체를 막지 않게 건너뛴다.
			slog.ErrorContext(ctx, "이벤트 직렬화 실패", "topic", topic, "req", e.RequestID, "err", err)
			continue
		}
		msgs = append(msgs, kafkago.Message{
			Topic: topic,
			// Key = requestId. 같은 사용자의 입장·회수가 같은 파티션에서 순서대로 처리된다.
			Key:     []byte(e.RequestID),
			Value:   b,
			Headers: []kafkago.Header(carrier),
		})
	}
	if len(msgs) == 0 {
		return nil
	}

	err := p.w.WriteMessages(ctx, msgs...)
	if err != nil {
		n := PublishFailures.Add(int64(len(msgs)))
		slog.ErrorContext(ctx, "발행 실패", "topic", topic, "batch", len(msgs), "total_failures", n, "err", err)
	} else {
		slog.InfoContext(ctx, "발행", "topic", topic, "batch", len(msgs))
	}
	return err
}

func (p *Producer) Close() error { return p.w.Close() }

// ensureTopic = 컨슈머가 구독하기 전에 토픽이 반드시 존재하도록 멱등 생성.
// kafka-go 그룹 리더는 "없는 토픽"에 먼저 붙으면, 토픽이 나중에 생겨도
// 다시 못 잡고 wedge되는 함정이 있다(cold start 레이스). → 구독 전 생성/대기.
// 부수효과로 broker가 뜰 때까지 Dial 재시도 = 기동 순서 의존성도 흡수.
func ensureTopic(ctx context.Context, broker, topic string) {
	for {
		if err := tryCreateTopic(broker, topic); err == nil {
			return
		}
		slog.WarnContext(ctx, "kafka 토픽 준비 대기", "topic", topic)
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func tryCreateTopic(broker, topic string) error {
	conn, err := kafkago.Dial("tcp", broker)
	if err != nil {
		return err
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	cc, err := kafkago.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer cc.Close()
	// ReplicationFactor -1 = 브로커 기본값(default.replication.factor)에 위임 —
	// 로컬 단일브로커=RF1, k3s 3브로커=RF3(GitOps KafkaTopic CR·브로커 config와 정합).
	// 하드코딩 RF1이면 3브로커에서도 복제 0 → 브로커 하나 죽으면 파티션 유실(#4).
	err = cc.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: -1})
	if err != nil && !strings.Contains(err.Error(), "exist") { // already exists = 정상
		return err
	}
	return nil
}

// ConsumeCompleted = "bookings-completed" 소비 → queue active에서 제거(자리 반환).
// booking이 예매 끝내면 이 이벤트가 와서 ③ 승격이 다음 대기자를 채운다.
//
// at-least-once: ReadMessage(받자마자 커밋 = 처리 전 죽으면
// 영구 유실)를 버리고 FetchMessage → 처리 성공 → CommitMessages(처리 후 커밋)로.
// 대가인 중복 재처리는 무해 — CompleteActive는 ZREM 한 줄이라 멱등.
func ConsumeCompleted(ctx context.Context, broker string, rdb *redis.Client) {
	ensureTopic(ctx, broker, TopicCompleted) // 구독 전 토픽 보장(wedge 방지)
	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: []string{broker},
		Topic:   TopicCompleted,
		GroupID: "queue",
	})
	defer r.Close()
	slog.InfoContext(ctx, "kafka consumer started (bookings-completed)")
	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.ErrorContext(ctx, "kafka completed fetch 실패", "err", err)
			time.Sleep(time.Second) // 백오프 — 브로커 장애 시 tight loop 방지
			continue
		}
		// trace: completed 헤더에서 W3C 컨텍스트를 extract해 producer(booking) trace에
		//   이어지는 consume span을 연다. IIFE+defer로 감싸 처리·커밋 규약(at-least-once)은 그대로 둔다.
		func() {
			carrier := kafkaHeaderCarrier(m.Headers)
			mctx := otel.GetTextMapPropagator().Extract(ctx, &carrier)
			sctx, span := otel.Tracer("queue-kafka").Start(mctx, "consume bookings-completed")
			defer span.End()

			var e Event
			if err := json.Unmarshal(m.Value, &e); err != nil {
				// 포이즌 메시지 — 재전달돼도 계속 실패하므로 커밋하고 스킵(추적 로그만).
				slog.WarnContext(sctx, "completed 메시지 파싱 실패(커밋 후 스킵)", "value", string(m.Value), "err", err)
				commit(ctx, r, m)
				return
			}
			removed, err := rdb.CompleteActive(sctx, e.MovieID, e.RequestID)
			if err != nil {
				// 처리 실패(Redis 순단 등) → 커밋하지 않는다: 재기동/리밸런스 때 재전달(at-least-once).
				// 같은 세션 안에서는 60s 세션 타임아웃이 자리를 회수(자가치유). [7] 꼬리 결정.
				n := ConsumeFailures.Add(1)
				slog.ErrorContext(sctx, "completeActive 실패(미커밋 — 재전달 대상)", "count", n, "req", e.RequestID, "movie", e.MovieID, "err", err)
				return
			}
			// 자리가 실제로 빈 뒤에 잰다 — 발행부터 여기까지가 회전 속도에 들어가는 구간이다.
			metrics.ObserveCompletedLag(sctx, time.Since(m.Time))
			slog.InfoContext(sctx, "예매완료 수신 → active 제거", "req", e.RequestID, "removed", removed)
			// 실황 피드(화면용) — 실제로 자리가 반환됐을 때만. best-effort.
			if removed {
				if fErr := rdb.AppendEvents(sctx, e.MovieID, "RETURN", []string{e.RequestID}, time.Now().UnixMilli()); fErr != nil {
					slog.WarnContext(sctx, "실황 기록 실패(표시만 영향)", "err", fErr)
				}
			}
			commit(ctx, r, m)
		}()
	}
}

// commit — 커밋 실패는 "같은 메시지가 나중에 한 번 더 옴"일 뿐(멱등이라 무해). 로그만.
func commit(ctx context.Context, r *kafkago.Reader, m kafkago.Message) {
	if err := r.CommitMessages(ctx, m); err != nil && ctx.Err() == nil {
		slog.WarnContext(ctx, "offset 커밋 실패(중복 재전달 무해)", "err", err)
	}
}
