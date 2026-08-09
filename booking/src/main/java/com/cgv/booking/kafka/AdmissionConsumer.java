package com.cgv.booking.kafka;

import com.cgv.booking.redis.AdmittedService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

// admissions 소비(queue→booking): "u3 입장했다" → 입장 인증 발급.
// GroupID=booking — 브로커가 이 그룹 offset 기억(죽어도 이어읽기).
// at-least-once라 중복 가능 → 인증 발급은 멱등이라 무해.
@Component
public class AdmissionConsumer {
    private static final Logger log = LoggerFactory.getLogger(AdmissionConsumer.class);
    private final AdmittedService admitted;
    private final ObjectMapper mapper;
    private final MeterRegistry meterRegistry;

    public AdmissionConsumer(AdmittedService admitted, ObjectMapper mapper, MeterRegistry meterRegistry) {
        this.admitted = admitted;
        this.mapper = mapper;
        this.meterRegistry = meterRegistry;
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
    @KafkaListener(topics = "admissions")   // groupId는 application.yml(consumer.group-id: booking) 단일 소스
    public void onAdmission(String message) {
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
        log.info("입장 인증 추가: movie={} req={}", e.movieId(), e.requestId());
    }
}
