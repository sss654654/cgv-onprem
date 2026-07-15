package com.cgv.booking.web;

import com.cgv.booking.service.BookingService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.web.bind.annotation.*;

import java.util.List;

// 결제 → 확정 → 완료(§3-1-5/6/7). "결제하기" 한 요청.
@RestController
@RequestMapping("/api/bookings")
public class BookingController {
    private final BookingService service;
    public BookingController(BookingService service) { this.service = service; }

    public record BookReq(@NotBlank String screeningId,
                          @NotEmpty List<String> seatNos,
                          @NotBlank String requestId,
                          @NotBlank String idempotencyKey) {}

    // @Valid 필수 — 없으면 @NotBlank/@NotEmpty가 죽은 코드가 되어
    // 빈 seatNos로 "좌석 0개·0원 예매"가 커밋될 수 있음.
    @PostMapping
    public BookingService.Result book(@Valid @RequestBody BookReq req) {
        return service.confirm(req.screeningId(), req.seatNos(), req.requestId(), req.idempotencyKey());
    }
}
