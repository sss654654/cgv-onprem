package com.cgv.booking.config;

import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.tracing.MicrometerTracing;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.boot.autoconfigure.data.redis.ClientResourcesBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// Redis 명령마다 span 을 남긴다.
//
// HTTP·Kafka·JDBC 는 Spring Boot 가 자동으로 계측하는데 Redis 만 빠져 있다. Boot 3.5 의
// LettuceMetricsAutoConfiguration 은 MicrometerCommandLatencyRecorder 를 쓰고, 그건 지표만
// 만들고 span 을 안 만든다. 자동으로 바뀌는 것은 Boot 4.0 부터다(spring-boot#46975).
//
// 없으면 예매 확정 트레이스에 구멍이 생긴다 — 입장 인증 확인·좌석락 획득·락 갱신·락 해제가
// 전부 Redis 인데, 그 구간이 HTTP span 안에 이름 없는 공백으로만 남아
// "느린 그 한 건이 좌석락에서 기다린 것인지 다른 데인지" 를 못 가른다.
// queue 는 같은 구간이 이미 보인다(go-redis 훅) — 그 비대칭을 없앤다.
@Configuration
public class RedisTracingConfig {

    // 명령 인자는 span 에 싣지 않는다(includeCommandArgs=false).
    // 좌석번호·requestId·멱등키가 그대로 값이라 카디널리티가 되고,
    // 예매 데이터가 트레이스 저장소로 나가는 경로가 된다. JDBC 쪽 파라미터를 끈 것과 같은 기준이다.
    @Bean
    ClientResourcesBuilderCustomizer lettuceTracingCustomizer(ObservationRegistry registry) {
        return (ClientResources.Builder builder) ->
                builder.tracing(new MicrometerTracing(registry, "redis", false));
    }
}
