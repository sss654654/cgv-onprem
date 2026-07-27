package com.cgv.booking.kafka;

import com.cgv.booking.redis.AdmittedService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

// 입장 인증 해소 소비(queue→booking): 세션 타임아웃·이탈로 queue가 자리를 회수했을 때
// booking의 입장 인증을 즉시 지운다.
//
// 왜 필요한가: 인증은 TTL로도 만료되지만(AdmittedService), TTL은 세션 타임아웃보다 길게 잡아야
// 정상 세션이 중간에 끊기지 않는다 → 그 차이만큼 "queue에서는 나갔는데 booking은 통과시키는" 창이
// 남는다. 이 이벤트가 그 창을 0으로 만든다. TTL = 최후 방어, 이 이벤트 = 즉시 해소.
//
// 발행 측 계약(queue와 합의됨):
//   토픽   : admissions-revoked (cgv.admission-revoked.topic으로 덮을 수 있음)
//   값     : {"requestId":"...","movieId":"...","reason":"SESSION_TIMEOUT|LEAVE|COMPLETE"}
//   키     : requestId — 같은 사용자의 입장·회수가 같은 파티션에서 순서대로 처리된다.
//            키가 없으면 회수가 입장보다 먼저 처리돼 이미 지운 인증이 되살아날 수 있다.
//   발행시점: active 세션 타임아웃 회수 / leave(이탈) / complete(자리 반환)
//   전달   : at-least-once. 처리(인증 삭제)는 멱등이라 중복 수신 무해.
@Component
@ConditionalOnProperty(name = "cgv.admission-revoked.enabled", havingValue = "true", matchIfMissing = true)
public class AdmissionExpiryConsumer {
    private static final Logger log = LoggerFactory.getLogger(AdmissionExpiryConsumer.class);
    private final AdmittedService admitted;
    private final ObjectMapper mapper;
    private final MeterRegistry meterRegistry;

    public AdmissionExpiryConsumer(AdmittedService admitted, ObjectMapper mapper, MeterRegistry meterRegistry) {
        this.admitted = admitted;
        this.mapper = mapper;
        this.meterRegistry = meterRegistry;
    }

    private void count(String result) {
        meterRegistry.counter("booking.admission.revoked", "result", result).increment();
    }

    @KafkaListener(topics = "${cgv.admission-revoked.topic:admissions-revoked}")
    public void onExpiry(String message) {
        QueueEvent e;
        try {
            e = mapper.readValue(message, QueueEvent.class);
        } catch (Exception ex) {
            count("parse_error");
            log.error("입장 해소 파싱 실패(스킵 — 그 인증은 TTL 만료까지 남는다): {} ({})", message, ex.getMessage());
            return;
        }
        if (e.requestId() == null || e.movieId() == null) {
            count("missing_field");
            log.error("입장 해소 필수 필드 누락(스킵 — 그 인증은 TTL 만료까지 남는다): {}", message);
            return;
        }
        admitted.remove(e.movieId(), e.requestId());   // 멱등(DEL). 실패 시 throw → 리스너 재시도
        count("ok");
        log.info("입장 인증 해소: movie={} req={}", e.movieId(), e.requestId());
    }
}
