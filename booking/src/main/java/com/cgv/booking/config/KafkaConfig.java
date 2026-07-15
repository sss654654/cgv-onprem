package com.cgv.booking.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

// admissions 소비 실패(주로 Redis 순단으로 SADD 실패) 처리 정책.
//
// 문제: Spring Kafka 기본 DefaultErrorHandler는 거의 즉시(간격 0) 10회 재시도 후 레코드를
//   스킵하고 오프셋을 커밋한다 → 몇 초짜리 Redis 순단(재기동 등)에도 입장 승인이 조용히 유실될 수
//   있다(admitted 미반영 → 그 유저는 좌석선택이 영구 403). "유실 없는 at-least-once" 주석의 전제가 깨짐.
// 처방: 고정 간격 재시도로 재시도 창을 약 40초까지 늘려 흔한 순단을 견디고, 그래도 소진되면 WARN이 아니라
//   ERROR로 크게 남겨 관측되게 한다(내가 모르는 요소 0). 완전 무손실은 무한 재시도/DLT가 필요하나,
//   파티션 무한 블록을 피하려 상한을 둔 절충이다.
// (Boot 오토컨피그가 CommonErrorHandler 빈을 리스너 컨테이너 팩토리에 자동 배선한다.)
@Configuration
public class KafkaConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        // 2s 간격 × 20회 ≈ 40s 재시도 창 — 흔한 Redis 순단(재기동 등)을 견딜 만큼.
        FixedBackOff backOff = new FixedBackOff(2000L, 20L);
        return new DefaultErrorHandler((record, ex) ->
                log.error("admissions 처리 최종 실패(재시도 소진 — 입장 승인 유실 위험, 수동 확인 필요): value={} err={}",
                        record.value(), ex.toString()),
                backOff);
    }
}
