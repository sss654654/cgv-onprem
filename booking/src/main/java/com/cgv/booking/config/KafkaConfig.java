package com.cgv.booking.config;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import org.apache.kafka.common.TopicPartition;

// admissions 소비 실패(주로 Redis 순단으로 인증 발급 실패) 처리 정책.
//
// 재시도는 리스너 스레드를 붙잡는다 = 그 컨슈머가 맡은 파티션 전체가 그동안 멈춘다.
// 그래서 재시도 창은 "흔한 순단을 넘길 만큼"으로만 잡고, 그 이상은 DLT로 격리해 파티션을 다시 흐르게 한다.
//   재시도 창 2s × 5 = 10s — Redis 파드 재기동(수 초)은 흡수하고, 파티션 정지는 10초로 묶인다.
//   (이전 값 2s × 20 = 40s는 한 건의 실패로 파티션을 40초 세운다. 그 40초 동안 승격된 사용자는 전부 좌석선택 403.)
// 소진분은 DLT 토픽으로 보낸다 — 메시지가 남아야 나중에 재처리할 수 있다. 카운터도 함께 올려
// "몇 건이 유실 경로로 갔는가"를 Grafana에서 셀 수 있게 한다(로그만으로는 못 센다).
// DLT 발행 자체가 실패해도(토픽 미생성 등) 삼키지 않고 ERROR로 남긴다.
//
// 운영 전제: DLT 토픽이 브로커에 있어야 한다. 이 에러핸들러는 리스너 공용이고 대상 토픽이
// "{원본 토픽}+접미사"라, 소비하는 토픽마다 DLT가 하나씩 필요하다 — admissions.DLT와
// admissions-revoked.DLT 두 개. 자동생성이 꺼진 클러스터에서는 토픽 선언(KafkaTopic)에
// 둘 다 있어야 하고, 없으면 DLT 발행 실패 로그로만 드러난다.
// (Boot 오토컨피그가 CommonErrorHandler 빈을 리스너 컨테이너 팩토리에 자동 배선한다.)
@Configuration
public class KafkaConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> template,
                                                 MeterRegistry meterRegistry,
                                                 @Value("${cgv.kafka.dlt-suffix:.DLT}") String dltSuffix) {
        DeadLetterPublishingRecoverer dlt = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new TopicPartition(record.topic() + dltSuffix, -1));   // 파티션 -1 = 브로커가 배정

        FixedBackOff backOff = new FixedBackOff(2000L, 5L);
        return new DefaultErrorHandler((record, ex) -> {
            meterRegistry.counter("booking.kafka.dlt", "topic", record.topic()).increment();
            log.error("{} 처리 최종 실패(재시도 소진 → DLT): value={} err={}", record.topic(), record.value(), ex.toString());
            try {
                dlt.accept(record, ex);
            } catch (RuntimeException dltEx) {
                log.error("DLT 발행 실패 — 이 메시지는 여기서 사라진다: topic={} value={} err={}",
                        record.topic(), record.value(), dltEx.toString());
            }
        }, backOff);
    }
}
