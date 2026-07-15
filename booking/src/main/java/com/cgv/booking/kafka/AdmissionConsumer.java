package com.cgv.booking.kafka;

import com.cgv.booking.redis.AdmittedService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

// admissions 소비(queue→booking): "u3 입장했다" → SADD admitted:{movieId} u3 (§2-4, §3-1-3).
// GroupID=booking — 브로커가 이 그룹 offset 기억(죽어도 이어읽기).
// at-least-once라 중복 가능 → SADD는 멱등이라 무해(§2-5).
@Component
public class AdmissionConsumer {
    private static final Logger log = LoggerFactory.getLogger(AdmissionConsumer.class);
    private final AdmittedService admitted;
    private final ObjectMapper mapper;

    public AdmissionConsumer(AdmittedService admitted, ObjectMapper mapper) {
        this.admitted = admitted;
        this.mapper = mapper;
    }

    // 파싱 실패와 처리(SADD) 실패를 구분한다.
    //   파싱 실패 = 포이즌 메시지 — 재시도해도 똑같이 실패 → 로그 남기고 스킵(오프셋 진행).
    //   SADD 실패 = 일시 장애(Redis 순단) — 예외를 전파해야 리스너가 재시도(오프셋 미커밋)
    //   → §2-5 "process → commit, 유실 없는 at-least-once"가 실제로 성립.
    @KafkaListener(topics = "admissions")   // groupId는 application.yml(consumer.group-id: booking) 단일 소스
    public void onAdmission(String message) {
        QueueEvent e;
        try {
            e = mapper.readValue(message, QueueEvent.class);
        } catch (Exception ex) {
            log.warn("admissions 파싱 실패(포이즌 메시지 스킵): {} ({})", message, ex.getMessage());
            return;
        }
        if (e.requestId() == null || e.movieId() == null) return;
        admitted.add(e.movieId(), e.requestId());   // 멱등(SADD). 실패 시 throw → 재시도
        log.info("입장 인증 추가: movie={} req={}", e.movieId(), e.requestId());
    }
}
