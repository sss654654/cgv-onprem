package com.cgv.booking.kafka;

import com.cgv.booking.config.CgvProps;
import com.cgv.booking.redis.AdmittedService;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

// 기동 직후, 트래픽을 받기 전에 admissions 소비 경로를 미리 돌려 JIT 를 데운다.
//
// 왜 필요한가 — 2026-09-13 stg 1만 명 판 실측. 오픈 순간 즉시입장 1,000건이 1초 안에 발행됐고
//   그 1,000건 전부가 인증까지 1초를 넘겼다(첫 배치를 집기까지 1.5–3.5초 · 배치 처리 124–1,020ms).
//   그 뒤 승격 루프가 보낸 9,000건은 1초 초과가 0건이었고 배치 처리는 25ms 였다. 같은 코드, 같은
//   부하인데 처음 1,000건만 느린 이유는 코드가 그때 처음 돌았기 때문이다 — 오픈 전 10분 동안
//   이 파드가 받은 것은 로비 폴링뿐이라 소비 경로(JSON 파싱 · Redis 파이프라인)는 인터프리터
//   상태였고, JIT 컴파일(jvm_compilation_time 1.1–2.2 코어/초)이 그 순간에 몰려 소비 스레드와
//   CPU 를 다퉜다.
//
// 어떻게 — ApplicationReadyEvent 에서 동기로 돈다. Spring Boot 는 이 이벤트를 다 처리한 뒤에야
//   ReadinessState 를 ACCEPTING_TRAFFIC 으로 올리므로(EventPublishingRunListener.ready), 예열이
//   끝나기 전에는 /actuator/health/readiness 가 503 이고 kubelet 이 이 파드를 Service 에 안 넣는다.
//   startupProbe 의 예산(5초 × 30회)이 상한이다.
//   레코드는 Kafka 를 거치지 않고 메모리에서 만들어 AdmissionConsumer.warm 에 직접 넣는다 —
//   브로커를 거치면 파티션 배정에 따라 다른 파드가 받을 수 있어 이 파드가 데워진다는 보장이 없다.
//   Kafka 클라이언트의 fetch 경로는 데워지지 않지만, 그 경로는 기동 직후부터 빈 poll 로 계속 돌고
//   있고 느렸던 구간은 그 뒤(파싱 · 발급)였다.
//   movieId 는 __warmup__ 이라 실제 영화의 인증과 키가 겹치지 않고, 끝나면 그 키를 지운다.
//   지표 · 스팬 · 로그는 남기지 않는다(AdmissionConsumer.warm 참조).
//
// 데우지 않는 것 — 좌석 선점(Lua) · 예매 확정(DB INSERT + Kafka 발행). 확정은 부작용 없이 돌릴 수
//   없다. 그 경로는 SLO 5 로 따로 보고, 오픈 순간에 느려지면 그때 이 컴포넌트를 넓힌다.
@Component
public class AdmissionWarmUp {
    private static final Logger log = LoggerFactory.getLogger(AdmissionWarmUp.class);
    static final String WARMUP_MOVIE = "__warmup__";
    // 실제 오픈 순간의 배치 크기(fetch.min.bytes 8KB 기준 49–80건)에 맞춘다.
    private static final int BATCH = 50;

    private final AdmissionConsumer consumer;
    private final AdmittedService admitted;
    private final int rounds;
    // booking_warmup_duration_seconds — 예열에 걸린 시간. -1 은 아직 안 돌았거나 껐다는 뜻.
    //   파드가 뜰 때마다 값이 하나라, 기동 시간에서 예열 몫이 얼마인지 대시보드에서 바로 읽는다.
    private final AtomicLong durationMs = new AtomicLong(-1000);

    public AdmissionWarmUp(AdmissionConsumer consumer, AdmittedService admitted, CgvProps props,
                           MeterRegistry meterRegistry) {
        this.consumer = consumer;
        this.admitted = admitted;
        this.rounds = props.getWarmupRounds();
        Gauge.builder("booking.warmup.duration", durationMs, v -> v.get() / 1000.0)
                .description("기동 예열에 걸린 시간. -1 = 안 돌았음")
                .baseUnit("seconds")
                .register(meterRegistry);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        if (rounds <= 0) {
            log.info("기동 예열 끔(warmup-rounds=0)");
            return;
        }
        long start = System.currentTimeMillis();
        try {
            List<ConsumerRecord<String, String>> batch = new ArrayList<>(BATCH);
            for (int i = 0; i < BATCH; i++) {
                String requestId = "warm-" + i;
                batch.add(new ConsumerRecord<>("admissions", 0, i, requestId,
                        "{\"requestId\":\"" + requestId + "\",\"movieId\":\"" + WARMUP_MOVIE + "\"}"));
            }
            for (int r = 0; r < rounds; r++) {
                consumer.warm(batch);
                // 게이트 검사(EXISTS)도 같이 — 좌석 조회 · 선점 · 확정이 전부 이 검사로 시작한다.
                admitted.isAdmitted(WARMUP_MOVIE, "warm-0");
            }
            for (int i = 0; i < BATCH; i++) {
                admitted.remove(WARMUP_MOVIE, "warm-" + i);
            }
            long elapsed = System.currentTimeMillis() - start;
            durationMs.set(elapsed);
            log.info("기동 예열 완료: rounds={} records={} elapsed={}ms", rounds, rounds * BATCH, elapsed);
        } catch (RuntimeException ex) {
            // 예열은 성능의 문제이지 정합의 문제가 아니다 — 실패해도 기동은 계속한다.
            durationMs.set(System.currentTimeMillis() - start);
            log.warn("기동 예열 실패(기동은 계속): {}", ex.toString());
        }
    }
}
