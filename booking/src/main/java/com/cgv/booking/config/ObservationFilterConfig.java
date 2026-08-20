package com.cgv.booking.config;

import io.micrometer.observation.ObservationPredicate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.server.observation.ServerRequestObservationContext;

// /actuator 요청은 관측하지 않는다 — 지표도 트레이스도 만들지 않는다.
//
// 프로브와 스크레이프는 요청 수가 적은데도 저장되는 exemplar 는 가장 많다. exemplar 는
// 스크레이프마다 버킷당 하나씩 저장되므로 개수가 트래픽이 아니라 (버킷 수 × 스크레이프 횟수)에
// 비례하기 때문이다. 실측(2026-08-20, 정원 300 · 8분 부하):
//
//   /actuator/health/**      요청   1,400건 → exemplar 734개   요청당 52.4%
//   /actuator/prometheus     요청     559건 → exemplar 367개   요청당 65.7%
//   /api/screenings          요청  15,689건 → exemplar 115개   요청당  0.73%
//
// booking 전체 exemplar 의 74%가 이 둘이었다. Mimir 의 exemplar 버퍼(100,000)와 Tempo 저장을
// 그만큼 차지하고, 표본이 1.0 이라 프로브마다 트레이스도 하나씩 생긴다.
//
// 지표를 잃는 대가는 없다. 대시보드는 네 패널 모두 uri!~"/actuator.*" 로 이미 빼고 있고,
// 프로브 실패는 kube_pod_status_ready 가, 스크레이프 지연은 Prometheus 가 자동으로 만드는
// scrape_duration_seconds 가 더 정확하게 잰다.
//
// 샘플러에서 경로로 거르는 방법을 쓰지 않는 이유: span 이름(http get /actuator/health/**)은
// URI 패턴이 확정된 뒤 붙는데 샘플러는 span 시작 시점에 불린다. 그 시점 이름이 최종 이름과
// 같다는 보장이 없어, 안 걸려도 증상이 없는 채로 조용히 통과한다.
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
