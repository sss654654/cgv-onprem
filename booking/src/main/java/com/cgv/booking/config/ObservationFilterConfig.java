package com.cgv.booking.config;

import io.micrometer.observation.ObservationPredicate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.server.observation.ServerRequestObservationContext;

// /actuator 요청을 관측 대상에서 제외한다 — 지표·트레이스 모두 생성하지 않는다.
//
// exemplar 는 스크레이프마다 히스토그램 버킷당 하나씩 저장되므로, 개수가 요청 수가 아니라
// (버킷 수 × 스크레이프 횟수)에 비례한다. 2026-08-20 정원 500 · 8분 부하 측정:
//
//   /actuator/health/**      요청   1,400건 → exemplar 734개   요청당 52.4%
//   /actuator/prometheus     요청     559건 → exemplar 367개   요청당 65.7%
//   /api/screenings          요청  15,689건 → exemplar 115개   요청당  0.73%
//
// booking 전체 exemplar 의 74%가 위 두 경로다. Mimir 의 exemplar 저장은 원형 버퍼이므로
// 한도(max_global_exemplars_per_user)를 넘으면 오래된 것이 밀려난다.
// 표본이 1.0 이므로 프로브 요청마다 트레이스도 생성된다.
//
// 제외해도 참조가 끊기는 화면이 없다 — booking 대시보드의 네 패널은 uri!~"/actuator.*" 로
// 이미 필터한다. 프로브 상태는 kube_pod_status_ready, 스크레이프 지연은
// Prometheus 가 생성하는 scrape_duration_seconds 로 측정된다.
//
// 샘플러 대신 ObservationPredicate 를 쓰는 이유: span 이름(http get /actuator/health/**)은
// URI 패턴 확정 후 설정되고, 샘플러는 span 생성 시점에 호출된다. 두 시점의 이름이
// 일치한다는 보장이 없다.
@Configuration
public class ObservationFilterConfig {

    @Bean
    ObservationPredicate excludeActuatorRequests() {
        return (name, context) -> {
            if (context instanceof ServerRequestObservationContext serverContext) {
                return !serverContext.getCarrier().getRequestURI().startsWith("/actuator");
            }
            return true;
        };
    }
}
