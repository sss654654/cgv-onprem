package com.cgv.booking.kafka;

import com.cgv.booking.redis.AdmittedService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

// admissions 소비(queue→booking): "u3 입장했다" → 입장 인증 발급.
// GroupID=booking — 브로커가 이 그룹 offset 기억(죽어도 이어읽기).
// at-least-once라 중복 가능 → 인증 발급은 멱등이라 무해.
// final — 생성자에서 예외가 나가면 부분 초기화된 객체가 남는다(SpotBugs CT_CONSTRUCTOR_THROW).
// 상속을 막으면 그 객체를 붙잡을 방법이 없어진다. @KafkaListener 는 프록시가 필요 없어
// 클래스를 final 로 둬도 스프링 쪽 동작은 그대로다.
@Component
public final class AdmissionConsumer {
    private static final Logger log = LoggerFactory.getLogger(AdmissionConsumer.class);
    private static final String TRACEPARENT = "traceparent";

    private final AdmittedService admitted;
    private final ObjectMapper mapper;
    private final MeterRegistry meterRegistry;
    private final Timer lag;
    private final Timer queueWait;
    private final Timer handle;
    private final DistributionSummary batchSize;
    private final Tracer tracer;
    // 이 시간을 넘게 기다린 레코드만 span 과 경고 로그를 남긴다.
    // 전부 남기면 오픈 순간에 span 이 레코드 수만큼 늘어 수집이 한도에 닿는다 — 실제로
    //   2026-09-12 stg 1만 명 판에서 Tempo 가 live_traces_exceeded 로 40,895 span 을 버렸고,
    //   하필 버려진 구간이 느린 구간이라 화면에서 exemplar 를 눌러도 트레이스가 없었다.
    //   느린 것만 남기면 양은 수백 건이고, 보고 싶은 것은 다 남는다.
    // 500ms 인 이유: 전파 목표가 1초라 그 절반을 넘으면 이미 볼 값이다.
    private static final long SLOW_WAIT_MS = 500;

    public AdmissionConsumer(AdmittedService admitted, ObjectMapper mapper, MeterRegistry meterRegistry,
                             ObjectProvider<Tracer> tracerProvider) {
        this.admitted = admitted;
        this.mapper = mapper;
        this.meterRegistry = meterRegistry;
        // 트레이서가 없으면 no-op 으로 떨어진다 — 계측 배선이 빠져도 소비는 계속돼야 한다.
        this.tracer = tracerProvider.getIfAvailable(() -> TracerProvider.noop().get("booking-admissions"));
        // booking_admission_lag_seconds — queue가 승격을 발행한 시각부터 여기서 인증이 생긴 시각까지.
        // 이 값이 있어야 "정원 30인데 실제 동시 예매가 20"의 원인에서 전달 지연을 지울 수 있다.
        // 두 서비스의 발행/소비 건수는 각각 지표로 나오지만, 한 건이 건너오는 데 걸린 시간은
        //   어느 지표에도 없다.
        // 10초까지는 http.server.requests와 같은 눈금이고, 그 위는 이 지표에만 있다.
        // 상한을 10초에서 60초로 넓힌 이유 — 2026-08-21 정원 500 부하에서 p50·p99가 모두
        //   10초(당시 최상단 버킷)에 붙었다. 최상단에 몰리면 분위수가 그 경계값으로 나와,
        //   실제가 10초인지 40초인지 구분이 안 되는 채로 화면에는 "10초"가 뜬다.
        //   같은 판에서 /api/screenings 의 403이 200의 2.4배였다(승격 9.0/초 · 200 9.48/초 ·
        //   403 22.71/초). 자리를 받고 인증을 기다리는 동안 게이트에 막혀 재시도한 것이고,
        //   그 대기 시간이 이 지표다.
        // 60초를 상한으로 두는 근거는 세션 타임아웃(SESSION_TIMEOUT=60)이다. 전달이 그보다
        //   길어지면 그 사용자는 이미 회수되므로, 그 위를 더 나눠도 판단이 달라지지 않는다.
        // 반대 방향과 비교하면 원인 범위가 좁아진다. 같은 판에서 queue_completed_lag_seconds
        //   (확정 발행 → 자리 반환 처리)는 p50 0.032초 · p99 0.080초였다. 같은 Kafka 를 쓰는데
        //   방향에 따라 자릿수가 다르므로, 느린 것은 브로커가 아니라 admissions 경로 쪽이다.
        this.lag = Timer.builder("booking.admission.lag")
                .description("승격 발행(queue) → 입장 인증 발급(booking) 지연")
                .serviceLevelObjectives(
                        Duration.ofMillis(1), Duration.ofMillis(5), Duration.ofMillis(10),
                        Duration.ofMillis(25), Duration.ofMillis(50), Duration.ofMillis(100),
                        Duration.ofMillis(250), Duration.ofMillis(500), Duration.ofSeconds(1),
                        Duration.ofMillis(2500), Duration.ofSeconds(5), Duration.ofSeconds(10),
                        Duration.ofSeconds(20), Duration.ofSeconds(30), Duration.ofSeconds(60))
                .register(meterRegistry);
        // booking_admission_wait_seconds — 위 lag 중 "이 파드가 그 레코드를 손에 쥐기 전"까지.
        // lag 하나로는 느릴 때 어디가 느린지 안 갈린다. lag = 대기 + 처리이고, 처리는
        //   배치 전체가 Redis 파이프라인 한 번이라 묶음당 한 값이다. 둘을 따로 재면
        //   lag - wait 가 처리 시간이 되어 뺄셈 하나로 갈린다.
        // 2026-09-12 stg 1만 명 판에서 이것이 필요해졌다 — lag p99 가 4.96초인데 발행 호출은
        //   979ms, 소비 처리 span 은 500ms 미만이었다. 남은 3초가 어느 구간인지 지표로 못 갈렸고,
        //   트레이스는 발행과 소비가 링크로만 이어져 한 화면에 그 틈이 안 나온다.
        // 눈금은 lag 와 같게 둔다 — 둘을 같은 그래프에 겹쳐 보려면 버킷 경계가 같아야 한다.
        this.queueWait = Timer.builder("booking.admission.wait")
                .description("승격 발행(queue) → 이 파드가 그 레코드를 배치로 받은 시점")
                .serviceLevelObjectives(
                        Duration.ofMillis(1), Duration.ofMillis(5), Duration.ofMillis(10),
                        Duration.ofMillis(25), Duration.ofMillis(50), Duration.ofMillis(100),
                        Duration.ofMillis(250), Duration.ofMillis(500), Duration.ofSeconds(1),
                        Duration.ofMillis(2500), Duration.ofSeconds(5), Duration.ofSeconds(10),
                        Duration.ofSeconds(20), Duration.ofSeconds(30), Duration.ofSeconds(60))
                .register(meterRegistry);
        // booking_admission_handle_seconds — 배치를 받은 뒤 인증을 다 만들기까지.
        // 위 둘을 빼서 구하지 않고 따로 잰다. lag 과 wait 은 각각 히스토그램이라 분위수끼리
        //   빼면 서로 다른 레코드의 값을 빼는 셈이 된다 — 2026-09-12 판에서 실제로 그렇게 읽다가
        //   "처리 4ms" 라는 없는 수를 만들었다. 직접 재면 그 계산이 필요 없다.
        // 눈금을 앞쪽으로 당겼다. 이 구간은 Redis 파이프라인 한 번이라 정상값이 수 ms 이고,
        //   lag 과 같은 눈금(첫 칸 1ms)이면 분위수가 첫 칸에 붙어 아무것도 안 갈린다.
        this.handle = Timer.builder("booking.admission.handle")
                .description("배치 수신 → 입장 인증 발급 완료")
                .serviceLevelObjectives(
                        Duration.ofMillis(1), Duration.ofMillis(2), Duration.ofMillis(5),
                        Duration.ofMillis(10), Duration.ofMillis(25), Duration.ofMillis(50),
                        Duration.ofMillis(100), Duration.ofMillis(250), Duration.ofMillis(500),
                        Duration.ofSeconds(1), Duration.ofMillis(2500), Duration.ofSeconds(5))
                .register(meterRegistry);
        // booking_admission_batch_size — poll 한 번이 가져온 레코드 수.
        // 소비 비용이 건당이 아니라 배치당이라(오프셋 커밋 · 호출 · Redis 왕복이 묶음당 1회)
        //   같은 유입이라도 배치가 잘게 쪼개지면 배출이 그만큼 떨어진다.
        //   2026-09-12 판에서 배치가 1-4건이었고 1건짜리도 34ms 가 걸려 배출이 초당 270건에
        //   묶였는데, 그 사실을 트레이스를 열어서야 알았다. 지표로 두면 판마다 바로 보인다.
        this.batchSize = DistributionSummary.builder("booking.admission.batch.size")
                .description("admissions poll 한 번이 가져온 레코드 수")
                .serviceLevelObjectives(1, 2, 5, 10, 25, 50, 100, 250, 500)
                .register(meterRegistry);
    }

    // booking_admissions_total{result=...} — 소비 종착점별 카운트.
    // skipped(파싱 실패·필드 누락)를 세지 않으면 "입장했는데 좌석선택 403"의 건수를 사후에 셀 수 없다.
    private void count(String result) {
        meterRegistry.counter("booking.admissions", "result", result).increment();
    }

    // 배치 소비 — poll이 가져온 레코드 묶음을 한 번에 처리한다(팩토리는 KafkaConfig의 배치 전용).
    //
    // 건별 소비(레코드당 호출 1회 + Redis 왕복 1회)는 소비 속도의 상한이 초당 약 28건이었고,
    // 예매 오픈 순간 admissions 수백 건이 몰리면 뒤쪽 레코드의 인증이 최대 10초 밀렸다
    // (2026-08-29 사용자 10,000 부하: 인증 지연 p99 9.94초, 발행·브로커·건별 처리 자체는 전부 정상).
    // 묶음 처리로 호출과 Redis 왕복이 묶음당 1회가 된다 — AdmittedService.addAll(파이프라인)과 짝.
    //
    // 파싱 실패와 처리 실패를 구분한다.
    //   파싱 실패·필드 누락 = 포이즌 메시지 — 재시도해도 똑같이 실패 → 카운터+ERROR 후 스킵.
    //     배치 안에서 걸러내므로 포이즌 하나가 정상 레코드 묶음을 재시도로 끌고 가지 않는다.
    //   인증 발급 실패(Redis 순단) = 예외 전파 → 배치 전체 재시도(오프셋 미커밋).
    //     발급은 멱등(TTL 재설정)이라 이미 성공한 레코드가 다시 처리돼도 무해하다.
    // publishedAts = 카프카 레코드 타임스탬프. 기본 CreateTime이라 발행 측(queue) 파드의 시계로 찍힌다.
    //   두 노드의 시계 차이가 그대로 오차로 섞인다(NTP 동기 기준 수 ms). 시계가 역전되면
    //   음수가 나오는데, 그건 지연이 아니라 잡음이라 버린다.
    // 지연은 addAll이 돌아온 뒤에 잰다 — "이 사용자가 좌석 선택을 통과할 수 있게 된 시점"까지가
    //   재려는 구간이라, 배치의 모든 레코드가 같은 완료 시각을 쓴다.
    // 스팬을 직접 연다. Spring Kafka 의 리스너 관측(spring.kafka.listener.observation-enabled)은
    //   레코드 리스너에만 붙고 배치 리스너는 지나간다 — 그래서 이 메서드 안에서 기록하는
    //   booking_admission_lag_seconds 와 booking_admissions_total 에 trace_id 가 없었다.
    //   같은 서비스의 레코드 리스너(AdmissionExpiryConsumer)가 세는
    //   booking_admission_revoked_total 에는 exemplar 가 붙는데 이 둘만 안 붙던 이유다.
    //   exemplar 가 없으면 그래프의 봉우리에서 트레이스로 넘어갈 수 없고, 인증 지연이
    //   "파티션에서 기다린 시간"인지 "처리에 쓴 시간"인지 지표만으로는 안 갈린다.
    // 링크로 잇는 이유: 배치 한 번에 서로 다른 트레이스의 레코드가 섞여 들어온다.
    //   부모는 하나뿐이라 배치에는 맞지 않고, 링크는 여러 개를 걸 수 있다.
    //   발행 측(queue)이 traceparent 를 헤더에 실어 보내므로 승격 → 발행 → 소비가 이어진다.
    @KafkaListener(topics = "admissions", containerFactory = "batchKafkaListenerContainerFactory")
    // groupId는 application.yml(consumer.group-id: booking) 단일 소스
    public void onAdmissions(List<ConsumerRecord<String, String>> records) {
        SpanBuilder builder = tracer.spanBuilder("admissions consume")
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute("messaging.system", "kafka")
                .setAttribute("messaging.destination.name", "admissions")
                .setAttribute("messaging.batch.message_count", records.size());
        for (ConsumerRecord<String, String> record : records) {
            SpanContext publisher = publisherContext(record);
            if (publisher != null) {
                builder.addLink(publisher);
            }
        }
        Span span = builder.startSpan();
        try (Scope ignored = span.makeCurrent()) {
            consume(records);
        } catch (RuntimeException ex) {
            span.recordException(ex);
            throw ex;
        } finally {
            span.end();
        }
    }

    // 헤더의 W3C traceparent(00-<트레이스 32자>-<스팬 16자>-<플래그 2자>)를 링크용 컨텍스트로.
    // 형식이 어긋나거나 헤더가 없으면 null — 계측 때문에 소비가 실패하지 않게 한다.
    private static SpanContext publisherContext(ConsumerRecord<String, String> record) {
        org.apache.kafka.common.header.Header header = record.headers().lastHeader(TRACEPARENT);
        if (header == null || header.value() == null) {
            return null;
        }
        String[] parts = new String(header.value(), StandardCharsets.UTF_8).split("-");
        if (parts.length < 4 || parts[1].length() != 32 || parts[2].length() != 16) {
            return null;
        }
        try {
            SpanContext context = SpanContext.createFromRemoteParent(
                    parts[1], parts[2], TraceFlags.fromHex(parts[3], 0), TraceState.getDefault());
            return context.isValid() ? context : null;
        } catch (RuntimeException ex) {
            return null;
        }
    }

    // 기다린 구간을 스팬 하나로 그린다. 시작·끝을 직접 넣어(발행 시각 → 배치를 쥔 시각)
    //   실제로 기다린 그 구간이 화면에서 막대로 보이게 한다. 이 구간은 지금까지 어느 스팬에도
    //   없었다 — 발행 스팬은 WriteMessages 가 돌아오면 끝나고, 소비 스팬은 처리를 시작할 때
    //   열려서, 그 사이 메시지가 파티션에 앉아 있던 시간이 두 스팬 사이 빈 곳으로 사라졌다.
    // 발행 스팬을 부모로 삼는다(링크가 아니라). 레코드 하나짜리라 부모가 하나로 정해지고,
    //   그래야 발행 → 대기 → 소비가 한 트레이스에서 이어져 보인다.
    // 파티션·오프셋을 붙이는 이유: 느린 것이 한 파티션에 몰리는지 흩어지는지가
    //   브로커 쪽과 컨슈머 쪽을 가르는 첫 갈림이다.
    private void waitSpan(ConsumerRecord<String, String> record, long publishedAt, long received, long waited) {
        SpanBuilder builder = tracer.spanBuilder("admissions queue wait")
                .setSpanKind(SpanKind.CONSUMER)
                .setStartTimestamp(publishedAt, TimeUnit.MILLISECONDS)
                .setAttribute("messaging.system", "kafka")
                .setAttribute("messaging.destination.name", "admissions")
                .setAttribute("messaging.kafka.destination.partition", record.partition())
                .setAttribute("messaging.kafka.message.offset", record.offset())
                .setAttribute("wait.ms", waited);
        SpanContext publisher = publisherContext(record);
        if (publisher != null) {
            builder.setParent(Context.root().with(Span.wrap(publisher)));
        }
        builder.startSpan().end(received, TimeUnit.MILLISECONDS);
    }

    private void consume(List<ConsumerRecord<String, String>> records) {
        consume(records, false);
    }

    // 기동 예열(AdmissionWarmUp)이 부른다. 파싱과 인증 발급은 실제 경로 그대로 돌리고, 그 뒤의
    //   지표 · 스팬 · 로그는 남기지 않는다. booking_admissions_total 은 queue 의 발행 건수와 같아야
    //   하는 불변식의 한쪽이고, 전파 지연 히스토그램에 예열 표본(0ms)이 섞이면 SLO 가 좋아 보인다.
    void warm(List<ConsumerRecord<String, String>> records) {
        consume(records, true);
    }

    private void consume(List<ConsumerRecord<String, String>> records, boolean warm) {
        // 이 배치를 손에 쥔 시각. 여기서 record.timestamp() 를 빼면 "발행된 뒤 집히기까지" 이고,
        //   아래 now 에서 이 값을 빼면 "집은 뒤 처리에 쓴 시간" 이다. 둘로 갈라야 느릴 때
        //   발행·브로커 쪽인지 이 파드 쪽인지 판단할 수 있다.
        long received = System.currentTimeMillis();
        List<AdmittedService.Admission> valid = new ArrayList<>(records.size());
        List<Long> validPublishedAts = new ArrayList<>(records.size());
        List<ConsumerRecord<String, String>> validRecords = new ArrayList<>(records.size());

        for (ConsumerRecord<String, String> record : records) {
            String message = record.value();
            QueueEvent e;
            try {
                e = mapper.readValue(message, QueueEvent.class);
            } catch (Exception ex) {
                count("parse_error");
                log.error("admissions 파싱 실패(스킵 — 이 사용자는 재입장 전까지 좌석선택 403): {} ({})", message, ex.getMessage());
                continue;
            }
            if (e.requestId() == null || e.movieId() == null) {
                count("missing_field");
                log.error("admissions 필수 필드 누락(스킵 — 이 사용자는 재입장 전까지 좌석선택 403): {}", message);
                continue;
            }
            valid.add(new AdmittedService.Admission(e.movieId(), e.requestId()));
            validPublishedAts.add(record.timestamp());
            validRecords.add(record);
        }

        if (!valid.isEmpty()) {
            admitted.addAll(valid);   // 멱등. 실패 시 throw → 배치 재시도
        }
        if (warm) {
            return;
        }

        long now = System.currentTimeMillis();
        batchSize.record(records.size());
        handle.record(now - received, TimeUnit.MILLISECONDS);
        int slowCount = 0;
        long slowestWait = 0;
        int slowestIndex = -1;
        for (int i = 0; i < valid.size(); i++) {
            count("ok");
            long publishedAt = validPublishedAts.get(i);
            long elapsed = now - publishedAt;
            if (elapsed >= 0) {
                lag.record(elapsed, TimeUnit.MILLISECONDS);
            }
            long waited = received - publishedAt;
            if (waited >= 0) {
                queueWait.record(waited, TimeUnit.MILLISECONDS);
                if (waited >= SLOW_WAIT_MS) {
                    slowCount++;
                    if (waited > slowestWait) {
                        slowestWait = waited;
                        slowestIndex = i;
                    }
                }
            }
        }
        // 배치에서 가장 오래 기다린 레코드 하나만 스팬으로 남긴다.
        //   레코드마다 남기면 오픈 순간 한 배치에서 수백 개가 만들어지고, 그 비용이 이 스레드에
        //   그대로 얹혀 다음 poll 이 늦어진다 — 재는 행위가 재려는 값을 키운다.
        //   2026-09-12 판에서 실제로 그랬다: admissions consume 스팬이 5초를 넘겼는데
        //   그 시간은 처리가 아니라 스팬 생성이었고, 전파 달성률이 95.05% 에서 89.99% 로 떨어졌다.
        //   한 배치 안의 레코드는 같은 파티션·비슷한 시각이라 가장 느린 하나로 구간이 드러난다.
        if (slowestIndex >= 0) {
            waitSpan(validRecords.get(slowestIndex), validPublishedAts.get(slowestIndex), received, slowestWait);
        }
        // 건별 info 로그는 두지 않는다 — 오픈 순간 초당 수백 줄이 되고, 건별 내용은 트레이스에 있다.
        log.info("입장 인증 추가: batch={} skipped={}", valid.size(), records.size() - valid.size());
        // 느린 배치만 한 줄 더. 파티션을 같이 적는 이유 — 특정 파티션만 느리면 그 리더 브로커나
        //   그 파티션을 맡은 컨슈머 하나의 문제이고, 전 파티션이 같이 느리면 발행 쪽이나 브로커 전체다.
        //   이 한 줄이 그 갈림을 로그만으로 세운다.
        if (slowCount > 0) {
            ConsumerRecord<String, String> first = validRecords.get(0);
            log.warn("입장 전달 지연: batch={} slow={} slowestWaitMs={} handleMs={} partition={} offset={}",
                    valid.size(), slowCount, slowestWait, now - received, first.partition(), first.offset());
        }
    }
}
