package com.cgv.booking.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

// 같은 내용이 반복해서 오는 경로가 성공했을 때는 접근 로그를 남기지 않는다.
//   /api/screenings/board  좌석 현황판 — 오픈을 기다리는 방문자 전원이 초 단위로 부른다
//   /actuator              kubelet 프로브 — 지표·트레이스에서도 이미 빼 둔 경로다
//
// 현황판은 응답 내용이 전원에게 같다. 2.5만 명 부하에서 6분 동안 301,353건이 들어왔고
// — booking 요청의 82% 다 — 접근 로그도 같은 수만큼 나갔다. 같은 판에서 클러스터 전체
// 로그가 초당 4,256줄이었고, 로그를 읽어 보내는 DaemonSet 이 노드마다 0.37–0.54 코어를 썼다.
//
// 잃는 것과 남는 것:
//   잃는다  성공한 현황판 요청의 개별 줄(출발지 IP 포함)
//   남는다  4xx·5xx 는 그대로 남는다 — 남용·오류는 여기서 드러난다
//           건수와 지연은 http_server_requests_seconds 가 uri 별로 계속 센다
//
// 구현: Tomcat 의 AccessLogValve 는 필터 체인 바깥에서 돌기 때문에, 체인이 끝난 뒤에
//   요청 속성을 붙여 두면 밸브가 그것을 보고 건너뛴다
//   (server.tomcat.accesslog.condition-unless 에 같은 이름을 적어 둔다).
@Configuration
public class AccessLogSkipConfig {

    // application.yml 의 server.tomcat.accesslog.condition-unless 와 같은 이름이어야 한다
    public static final String SKIP_ATTRIBUTE = "skipAccessLog";

    private static final String BOARD_PATH = "/api/screenings/board";
    private static final String ACTUATOR_PREFIX = "/actuator";

    private static boolean repeated(String uri) {
        return BOARD_PATH.equals(uri) || uri.startsWith(ACTUATOR_PREFIX);
    }

    @Bean
    FilterRegistrationBean<OncePerRequestFilter> skipPollingAccessLog() {
        OncePerRequestFilter filter = new OncePerRequestFilter() {
            @Override
            protected void doFilterInternal(HttpServletRequest request,
                                            HttpServletResponse response,
                                            FilterChain chain) throws ServletException, IOException {
                chain.doFilter(request, response);
                if (response.getStatus() < 400 && repeated(request.getRequestURI())) {
                    request.setAttribute(SKIP_ATTRIBUTE, Boolean.TRUE);
                }
            }
        };
        FilterRegistrationBean<OncePerRequestFilter> registration = new FilterRegistrationBean<>(filter);
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE);
        return registration;
    }
}
