package com.cgv.booking.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

// bookings-completed 발행(booking→queue): "u3 예매 끝, 자리 빼" → queue가 소비해 ZREM active (§3-1-7).
// 방향이 admissions의 반대 = 닫힌 순환(§4). 비동기(던지고 끝).
@Component
public class CompletedProducer {
    private static final Logger log = LoggerFactory.getLogger(CompletedProducer.class);
    private static final String TOPIC = "bookings-completed";
    private final KafkaTemplate<Object, Object> kafka;   // Boot 기본 ProducerFactory<Object,Object>와 매칭
    private final ObjectMapper mapper;

    public CompletedProducer(KafkaTemplate<Object, Object> kafka, ObjectMapper mapper) {
        this.kafka = kafka;
        this.mapper = mapper;
    }

    public void publishCompleted(String requestId, String movieId) {
        final String json;
        try {
            json = mapper.writeValueAsString(new QueueEvent(requestId, movieId));
        } catch (Exception e) {
            throw new RuntimeException("completed 직렬화 실패", e);
        }
        // send()는 비동기(CompletableFuture 반환) — 브로커 순단 등 "전송 단계" 실패는 이 콜백에서만 드러난다.
        // 동기 try-catch만으론 그 실패가 무음이 된다(내가 모르는 요소 0 위반). 예매는 이미 커밋됐으므로
        // 던지지 않고 ERROR로 관측만 한다(queue 자리 반납은 세션 타임아웃이 최후 회수).
        kafka.send(TOPIC, json).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("bookings-completed 발행 실패 — 자리 반납 지연(세션 타임아웃이 회수): req={} movie={} err={}",
                        requestId, movieId, ex.toString());
            }
        });
    }
}
