package com.cgv.booking.web;

import com.cgv.booking.service.ScreeningService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

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
    @GetMapping("/board")
    public List<ScreeningService.ScreeningView> board(@RequestParam String movieId) {
        return service.board(movieId);
    }
}
