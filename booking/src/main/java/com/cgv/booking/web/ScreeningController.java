package com.cgv.booking.web;

import com.cgv.booking.service.ScreeningService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

// 회차(관) 선택 화면 데이터(§3-1-1). 프론트가 branch로 그룹핑.
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
}
