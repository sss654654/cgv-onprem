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

	"cgv-onprem/queue-go/redis"
)

// kafkaHeaderCarrier = OTel TextMapCarrier를 kafka-go 메시지 헤더에 어댑트.
// W3C traceparent를 헤더로 주고받아 producer(booking·queue)→consumer trace를 잇는다(§7).
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
	TopicCompleted  = "bookings-completed" // booking → queue (자리 반환)
)

type Event struct {
	RequestID string `json:"requestId"`
	MovieID   string `json:"movieId"`
}

// PublishFailures = 발행 최종 실패(내부 재시도 소진 후) 누적. ConsumeFailures = completed
// 처리 실패(미커밋) 누적. 단일 진실원은 이 atomic이고, promauto CounterFunc가 이 값을 그대로
// 읽어 queue_kafka_publish/consume_failures_total로 노출한다(설계서 1부 §7-B 행3).
var (
	PublishFailures atomic.Int64
	ConsumeFailures atomic.Int64
)

// Producer = "admissions" 발행. 승격 시 호출 → booking이 admitted set을 채운다.
type Producer struct{ w *kafkago.Writer }

// NewProducer는 즉시 반환한다(기동 비동기화 — 설계서 1부 §2-B).
// 토픽 보장(ensureTopic)은 백그라운드로 뺐다: Kafka가 늦게 떠도 HTTP 포트는 열린다.
// 첫 발행은 AllowAutoTopicCreation + Writer 내부 재시도가 받치고, 그래도 실패하면
// 발행실패 처리(§3-A 정직한 실패)가 받는다. ctx = main의 루트 ctx — 종료 시 대기 중단.
func NewProducer(ctx context.Context, broker string) *Producer {
	go ensureTopic(ctx, broker, TopicAdmissions)
	return &Producer{w: &kafkago.Writer{
		Addr:                   kafkago.TCP(broker),
		Topic:                  TopicAdmissions,
		Balancer:               &kafkago.LeastBytes{},
		AllowAutoTopicCreation: true,
		// RequiredAcks 기본값(RequireNone)은 브로커 확인 없이 성공 반환 — "발행 실패 처리"가
		// 성립하려면 실패를 알아야 하므로 ack 필수.
		RequiredAcks: kafkago.RequireOne,
		MaxAttempts:  3, // 짧은 재시도(§3-A ①) — 몇 초 순단은 여기서 흡수
		// WriteTimeout은 "시도당" 상한이다 — kafka-go는 시도마다 새 타임아웃을 만들고
		// 재시도 사이에 백오프를 둔다. 최악 지연 ≈ 1s×3 + 백오프 = 약 5s로,
		// enter 경로의 서버 WriteTimeout(10s) 안쪽으로 묶인다.
		WriteTimeout: time.Second,
	}}
}

// PublishAdmission — processor·handler의 AdmissionPublisher 인터페이스를 만족.
// Writer 내부 재시도(MaxAttempts=3) 후에도 실패면 에러를 돌려준다. 그다음은 호출자의
// 정직한 실패(§3-A): enter=보상 롤백+재시도 안내, 승격 루프=롤백+승격 일시 중단.
func (p *Producer) PublishAdmission(ctx context.Context, requestID, movieID string) error {
	// trace(§7): 발행 span을 열고 W3C traceparent를 Kafka 헤더에 inject.
	//   enter 경로는 ctx에 HTTP 서버 span이 있어 그 자식으로, 승격 루프는 background(span 없음)라
	//   여기서 뜨는 span이 새 루트가 된다 — 두 발행 지점 모두 유효한 traceparent를 싣는다.
	ctx, span := otel.Tracer("queue-kafka").Start(ctx, "publish admissions")
	defer span.End()

	b, _ := json.Marshal(Event{RequestID: requestID, MovieID: movieID})
	var carrier kafkaHeaderCarrier
	otel.GetTextMapPropagator().Inject(ctx, &carrier)
	err := p.w.WriteMessages(ctx, kafkago.Message{Value: b, Headers: []kafkago.Header(carrier)})
	if err != nil {
		n := PublishFailures.Add(1)
		slog.ErrorContext(ctx, "admissions 발행 실패", "count", n, "req", requestID, "movie", movieID, "err", err)
	} else {
		slog.InfoContext(ctx, "admissions 발행", "req", requestID, "movie", movieID)
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
	err = cc.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
	if err != nil && !strings.Contains(err.Error(), "exist") { // already exists = 정상
		return err
	}
	return nil
}

// ConsumeCompleted = "bookings-completed" 소비 → queue active에서 제거(자리 반환).
// booking이 예매 끝내면 이 이벤트가 와서 ③ 승격이 다음 대기자를 채운다.
//
// at-least-once(설계서 1부 §3-B, 필수 7): ReadMessage(받자마자 커밋 = 처리 전 죽으면
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
		// trace(§7): completed 헤더에서 W3C 컨텍스트를 extract해 producer(booking) trace에
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
			removed, err := rdb.CompleteActive(ctx, e.MovieID, e.RequestID)
			if err != nil {
				// 처리 실패(Redis 순단 등) → 커밋하지 않는다: 재기동/리밸런스 때 재전달(at-least-once).
				// 같은 세션 안에서는 60s 세션 타임아웃이 자리를 회수(자가치유). [7] 꼬리 결정.
				n := ConsumeFailures.Add(1)
				slog.ErrorContext(sctx, "completeActive 실패(미커밋 — 재전달 대상)", "count", n, "req", e.RequestID, "movie", e.MovieID, "err", err)
				return
			}
			slog.InfoContext(sctx, "예매완료 수신 → active 제거", "req", e.RequestID, "removed", removed)
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
