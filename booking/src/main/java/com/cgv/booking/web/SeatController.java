package com.cgv.booking.web;

import com.cgv.booking.service.SeatService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotBlank;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

// 좌석도 조회(§3-1-2) + 점유(§3-1-3) + 해제(뒤로가기, §3-4).
@RestController
@RequestMapping("/api/seats")
public class SeatController {
    private final SeatService service;
    public SeatController(SeatService service) { this.service = service; }

    // 좌석도: 200칸 + taken(빈/임시점유/판매완료 합쳐 불린).
    @GetMapping
    public List<SeatService.SeatView> seatMap(@RequestParam String screeningId,
                                              @RequestParam String requestId) {
        return service.seatMap(screeningId, requestId);
    }

    public record SelectReq(@NotBlank String screeningId,
                            @NotEmpty List<String> seatNos,
                            @NotBlank String requestId) {}

    // 점유: 성공 200 LOCKED / 실패 409(이미 선점). @Valid = 빈 seatNos 차단.
    @PostMapping("/select")
    public Map<String, Object> select(@Valid @RequestBody SelectReq req) {
        service.select(req.screeningId(), req.seatNos(), req.requestId());
        return Map.of("status", "LOCKED", "seatNos", req.seatNos());
    }

    // 해제(← 다른 관/취소): 내 점유만 DEL.
    // ※ admitted 게이트는 의도적으로 없음 — 만료(EXPIRED)된 유저도 자기 락은 반환할 수 있어야
    //    유령 점유가 TTL까지 안 남음(일관성보다 회수 용이성).
    @PostMapping("/release")
    public Map<String, Object> release(@Valid @RequestBody SelectReq req) {
        service.release(req.screeningId(), req.seatNos(), req.requestId());
        return Map.of("status", "RELEASED");
    }
}
