package com.cgv.booking.web;

import com.cgv.booking.service.ScreeningService;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.concurrent.TimeUnit;

// 회차(관) 선택 화면 데이터. 프론트가 branch로 그룹핑.
@RestController
@RequestMapping("/api/screenings")
public class ScreeningController {
    private final ScreeningService service;
    public ScreeningController(ScreeningService service) { this.service = service; }

    @GetMapping
    public List<ScreeningService.ScreeningView> list(@RequestParam String movieId,
                                                     @RequestParam String requestId) {
        return service.listForMovie(movieId, requestId);   // 미입장이면 403(게이트)
    }

    // 좌석 현황판 — 입장 전에도 부를 수 있다(게이트 없음). 회차별 총/잔여 수만 나가고
    //   좌석 번호나 점유 주체는 담기지 않는다. 예매는 여전히 위의 게이트 뒤에서만 된다.
    //
    // Cache-Control 을 붙이는 이유 — 이 응답은 요청자와 무관하게 같고, 대기 중인 사람 전원이
    //   3초마다 부른다. 게이트 밖이라 정원(MAX_SESSIONS)이 이 부하를 막지 못하고 인원에
    //   정비례한다. 실측(2026-08-30 사용자 10,000): 오픈 직전 초당 3,234건이 들어와
    //   booking CPU 1.34/2코어를 차지했고, 그때 Kafka 리스너가 밀려 입장 인증 등록이
    //   19.86초까지 늘어졌다. HTTP 처리와 Kafka 소비가 같은 JVM·같은 2코어를 쓴다.
    //   인원당 초당 요청이 (인원 ÷ 3)이므로 2코어로는 약 14,500명이 상한이다.
    // max-age 5초 — 프론트 폴링 주기(live.js POLL_MS = 3000)보다 크게 잡는다. 같은 값이면
    //   캐시가 만료되는 순간과 다음 폴링이 겹쳐 origin 까지 가는 요청이 생긴다.
    //   엣지가 5초에 한 번만 origin 을 부르므로 인원이 늘어도 이 경로의 origin 부하는
    //   초당 0.2건으로 고정된다.
    // public 인 이유: 응답에 사용자별 내용이 없다. requestId 를 받지 않고 인증도 쿠키도 안 쓴다.
    // 잔여 수가 최대 5초(+ ScreeningService 의 서버 캐시 1초) 늦게 보인다. 실제 좌석 선택은
    //   게이트 뒤 /api/seats 라 캐시되지 않으므로 예매 정확성에는 영향이 없다.
    // ⚠ CDN 은 JSON 응답을 기본으로 캐시하지 않는다. 이 헤더만으로는 안 먹고
    //   엣지에 이 경로를 캐시 대상으로 지정하는 규칙이 함께 있어야 한다.
    @GetMapping("/board")
    public ResponseEntity<List<ScreeningService.ScreeningView>> board(@RequestParam String movieId) {
        return ResponseEntity.ok()
                .cacheControl(CacheControl.maxAge(5, TimeUnit.SECONDS).cachePublic())
                .body(service.board(movieId));
    }
}
