package com.cgv.booking.kafka;

import com.cgv.booking.redis.AdmittedService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

// admissions 소비(queue→booking): "u3 입장했다" → 입장 인증 발급.
// GroupID=booking — 브로커가 이 그룹 offset 기억(죽어도 이어읽기).
// at-least-once라 중복 가능 → 인증 발급은 멱등이라 무해.
@Component
public class AdmissionConsumer {
    private static final Logger log = LoggerFactory.getLogger(AdmissionConsumer.class);
    private final AdmittedService admitted;
    private final ObjectMapper mapper;
    private final MeterRegistry meterRegistry;
    private final Timer lag;

    public AdmissionConsumer(AdmittedService admitted, ObjectMapper mapper, MeterRegistry meterRegistry) {
        this.admitted = admitted;
        this.mapper = mapper;
        this.meterRegistry = meterRegistry;
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
    }

    // booking_admissions_total{result=...} — 소비 종착점별 카운트.
    // skipped(파싱 실패·필드 누락)를 세지 않으면 "입장했는데 좌석선택 403"의 건수를 사후에 셀 수 없다.
    private void count(String result) {
        meterRegistry.counter("booking.admissions", "result", result).increment();
    }

    // 파싱 실패와 처리 실패를 구분한다.
    //   파싱 실패·필드 누락 = 포이즌 메시지 — 재시도해도 똑같이 실패 → 카운터+ERROR 후 스킵(오프셋 진행).
    //   인증 발급 실패 = 일시 장애(Redis 순단) — 예외를 전파해야 리스너가 재시도(오프셋 미커밋)
    //   → "process → commit, 유실 없는 at-least-once"가 실제로 성립.
    // publishedAt = 카프카 레코드 타임스탬프. 기본 CreateTime이라 발행 측(queue) 파드의 시계로 찍힌다.
    //   그래서 두 노드의 시계 차이가 그대로 오차로 섞인다(NTP 동기 기준 수 ms). 시계가 역전되면
    //   음수가 나오는데, 그건 지연이 아니라 잡음이라 버린다.
    @KafkaListener(topics = "admissions")   // groupId는 application.yml(consumer.group-id: booking) 단일 소스
    public void onAdmission(String message,
                            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long publishedAt) {
        QueueEvent e;
        try {
            e = mapper.readValue(message, QueueEvent.class);
        } catch (Exception ex) {
            count("parse_error");
            log.error("admissions 파싱 실패(스킵 — 이 사용자는 재입장 전까지 좌석선택 403): {} ({})", message, ex.getMessage());
            return;
        }
        if (e.requestId() == null || e.movieId() == null) {
            count("missing_field");
            log.error("admissions 필수 필드 누락(스킵 — 이 사용자는 재입장 전까지 좌석선택 403): {}", message);
            return;
        }
        admitted.add(e.movieId(), e.requestId());   // 멱등. 실패 시 throw → 리스너 재시도
        count("ok");
        // 인증이 실제로 생긴 뒤에 잰다 — 전달만이 아니라 "이 사용자가 좌석 선택을 통과할 수 있게 된
        //   시점"까지가 회전 속도에 걸리는 구간이다.
        long elapsed = System.currentTimeMillis() - publishedAt;
        if (elapsed >= 0) {
            lag.record(elapsed, TimeUnit.MILLISECONDS);
        }
        log.info("입장 인증 추가: movie={} req={}", e.movieId(), e.requestId());
    }
}
