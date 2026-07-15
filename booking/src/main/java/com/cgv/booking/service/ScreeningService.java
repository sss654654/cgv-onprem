package com.cgv.booking.service;

import com.cgv.booking.config.CgvProps;
import com.cgv.booking.domain.Screening;
import com.cgv.booking.redis.AdmittedService;
import com.cgv.booking.redis.SeatLockService;
import com.cgv.booking.repo.BookingSeatRepository;
import com.cgv.booking.repo.ScreeningRepository;
import com.cgv.booking.web.ApiException;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

// 회차(관) 선택 화면(§3-1-1): 이 방송의 관 목록 + 각 잔여좌석.
// 잔여 = total − 임시점유(Redis) − 판매완료(MySQL). 두 출처 합산 필수(§3-1-1).
@Service
public class ScreeningService {
    private final ScreeningRepository screenings;
    private final BookingSeatRepository bookingSeats;
    private final SeatLockService locks;
    private final AdmittedService admitted;
    private final CgvProps props;

    public ScreeningService(ScreeningRepository screenings, BookingSeatRepository bookingSeats,
                            SeatLockService locks, AdmittedService admitted, CgvProps props) {
        this.screenings = screenings; this.bookingSeats = bookingSeats;
        this.locks = locks; this.admitted = admitted; this.props = props;
    }

    public record ScreeningView(String screeningId, String branch, int screenNo, int total, int remain) {}

    public List<ScreeningView> listForMovie(String movieId, String requestId) {
        // 게이트(§3-1-3 ①): 방송 입장객 아니면 403.
        if (!admitted.isAdmitted(movieId, requestId)) {
            throw ApiException.forbidden("입장객이 아닙니다(미승인). 대기열을 거쳐 입장하세요.");
        }
        List<Screening> list = screenings.findByMovieIdOrderByBranchAscScreenNoAsc(movieId);
        List<ScreeningView> out = new ArrayList<>(list.size());
        for (Screening s : list) {
            long sold = bookingSeats.countByScreeningId(s.getId());      // 판매완료(영구, MySQL)
            long locked = locks.countLocked(s.getId());                  // 임시점유(Redis)
            int remain = (int) Math.max(0, s.getTotalSeats() - sold - locked);
            out.add(new ScreeningView(s.getId(), s.getBranch(), s.getScreenNo(), s.getTotalSeats(), remain));
        }
        return out;
    }
}
