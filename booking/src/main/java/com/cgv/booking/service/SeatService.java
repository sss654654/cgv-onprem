package com.cgv.booking.service;

import com.cgv.booking.domain.Screening;
import com.cgv.booking.redis.AdmittedService;
import com.cgv.booking.redis.SeatLockService;
import com.cgv.booking.repo.BookingSeatRepository;
import com.cgv.booking.repo.ScreeningRepository;
import com.cgv.booking.repo.SeatRepository;
import com.cgv.booking.web.ApiException;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

// 좌석도 조회(§3-1-2) + 좌석 점유(§3-1-3).
// 좌석 상태 3개(빈/임시점유=Redis/판매완료=MySQL) → 화면엔 taken 불린으로 합쳐 전달.
@Service
public class SeatService {
    private final SeatRepository seats;
    private final ScreeningRepository screenings;
    private final BookingSeatRepository bookingSeats;
    private final SeatLockService locks;
    private final AdmittedService admitted;

    public SeatService(SeatRepository seats, ScreeningRepository screenings, BookingSeatRepository bookingSeats,
                       SeatLockService locks, AdmittedService admitted) {
        this.seats = seats; this.screenings = screenings; this.bookingSeats = bookingSeats;
        this.locks = locks; this.admitted = admitted;
    }

    public record SeatView(String seatNo, int row, int col, boolean taken) {}

    private String movieIdOf(String screeningId) {
        Screening s = screenings.findById(screeningId)
                .orElseThrow(() -> new ApiException(org.springframework.http.HttpStatus.NOT_FOUND, "NO_SCREENING", "회차 없음"));
        return s.getMovieId();
    }

    // 좌석도: 구조(MySQL) + taken(판매완료 ∪ 임시점유).
    public List<SeatView> seatMap(String screeningId, String requestId) {
        String movieId = movieIdOf(screeningId);
        if (!admitted.isAdmitted(movieId, requestId)) throw ApiException.forbidden("입장객이 아닙니다.");

        Set<String> sold = bookingSeats.findByScreeningId(screeningId).stream()
                .map(bs -> bs.getSeatNo()).collect(Collectors.toSet());
        Set<String> locked = locks.lockedSeatNos(screeningId);

        return seats.findByScreeningIdOrderBySeatRowAscSeatColAsc(screeningId).stream()
                .map(seat -> new SeatView(seat.getSeatNo(), seat.getSeatRow(), seat.getSeatCol(),
                        sold.contains(seat.getSeatNo()) || locked.contains(seat.getSeatNo())))
                .toList();
    }

    // 좌석 점유(§3-1-3): 게이트 → SET NX(다중=Lua all-or-nothing). 실패=409.
    public void select(String screeningId, List<String> seatNos, String requestId) {
        String movieId = movieIdOf(screeningId);
        if (!admitted.isAdmitted(movieId, requestId)) throw ApiException.forbidden("입장객이 아닙니다.");
        boolean ok = locks.lockAll(screeningId, seatNos, requestId);
        if (!ok) throw ApiException.conflict("이미 선점된 좌석이 있습니다.");
    }

    // 뒤로가기/취소 — 내 점유 해제(§3-1-3, 3-4 네비게이션).
    public void release(String screeningId, List<String> seatNos, String requestId) {
        locks.releaseMine(screeningId, seatNos, requestId);
    }
}
