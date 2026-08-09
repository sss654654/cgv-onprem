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

// 좌석도 조회 + 좌석 점유.
// 좌석 상태 3개(빈/임시점유=Redis/판매완료=MySQL) → 화면엔 taken 불린으로 합쳐 전달.
@Service
public class SeatService {
    private final SeatRepository seats;
    private final ScreeningRepository screenings;
    private final BookingSeatRepository bookingSeats;
    private final SeatLockService locks;
    private final AdmittedService admitted;
    private final SeatRequest seatRequest;

    public SeatService(SeatRepository seats, ScreeningRepository screenings, BookingSeatRepository bookingSeats,
                       SeatLockService locks, AdmittedService admitted, SeatRequest seatRequest) {
        this.seats = seats; this.screenings = screenings; this.bookingSeats = bookingSeats;
        this.locks = locks; this.admitted = admitted; this.seatRequest = seatRequest;
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

    // 좌석 점유: 게이트 → 좌석 검증 → SET NX(다중=Lua all-or-nothing). 실패=409.
    public void select(String screeningId, List<String> rawSeatNos, String requestId) {
        String movieId = movieIdOf(screeningId);
        List<String> seatNos = seatRequest.normalize(rawSeatNos);
        if (!admitted.isAdmitted(movieId, requestId)) throw ApiException.forbidden("입장객이 아닙니다.");
        // 실재 검증: 없는 좌석번호도 Redis에는 키가 없어 점유가 성공한다 → 결제·확정까지 통과하고
        // 그 행이 잔여좌석을 깎아 정상 사용자에게 매진으로 보인다.
        seatRequest.ensureSeatsExist(screeningId, seatNos);
        // 판매완료 검증: 확정 직후 좌석락을 지우므로 판매완료 좌석은 Redis에 흔적이 없다.
        // 여기서 안 막으면 이미 팔린 좌석이 다시 잠기고 결제까지 진행된 뒤 UNIQUE 위반으로 환불된다.
        seatRequest.ensureNotSold(screeningId, seatNos);
        boolean ok = locks.lockAll(screeningId, seatNos, requestId);
        if (!ok) throw ApiException.conflict("이미 선점된 좌석이 있습니다.");
    }

    // 뒤로가기/취소 — 내 점유 해제.
    // 실재·판매 검증은 하지 않는다 — 해제는 "내 값인 키만 DEL"이라 남에게 영향이 없고,
    // 회수는 항상 열려 있어야 유령 점유가 TTL까지 남지 않는다. 개수 상한만 적용한다.
    public void release(String screeningId, List<String> rawSeatNos, String requestId) {
        locks.releaseMine(screeningId, seatRequest.normalize(rawSeatNos), requestId);
    }
}
