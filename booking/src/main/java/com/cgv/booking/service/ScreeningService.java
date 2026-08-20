package com.cgv.booking.service;

import com.cgv.booking.domain.Screening;
import com.cgv.booking.redis.AdmittedService;
import com.cgv.booking.redis.SeatLockService;
import com.cgv.booking.repo.BookingSeatRepository;
import com.cgv.booking.repo.ScreeningRepository;
import com.cgv.booking.web.ApiException;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// 회차(관) 선택 화면: 이 방송의 관 목록 + 각 잔여좌석.
// 잔여 = total − 임시점유(Redis) − 판매완료(MySQL). 두 출처 합산 필수.
@Service
public class ScreeningService {
    private final ScreeningRepository screenings;
    private final BookingSeatRepository bookingSeats;
    private final SeatLockService locks;
    private final AdmittedService admitted;

    public ScreeningService(ScreeningRepository screenings, BookingSeatRepository bookingSeats,
                            SeatLockService locks, AdmittedService admitted) {
        this.screenings = screenings; this.bookingSeats = bookingSeats;
        this.locks = locks; this.admitted = admitted;
    }

    public record ScreeningView(String screeningId, String branch, int screenNo, int total, int remain) {}

    // 이 화면은 입장 직후 첫 화면이라 정원 전체가 주기적으로 다시 부른다 = 요청당 왕복 수가 그대로 부하가 된다.
    //   판매수: 회차마다 count 쿼리를 돌리지 않고 group by 한 번으로 전부 받는다(JDBC 왕복 2회).
    //   점유수: 회차별 인덱스 Set을 읽는다(회차당 Redis 왕복 1회). 이 Redis는 queue가 대기열 폴링을
    //           처리하는 인스턴스와 같아서, 여기서 왕복이 늘면 queue의 지연이 먼저 무너진다.
    public List<ScreeningView> listForMovie(String movieId, String requestId) {
        // 게이트: 방송 입장객 아니면 403.
        if (!admitted.isAdmitted(movieId, requestId)) {
            throw ApiException.forbidden("입장객이 아닙니다(미승인). 대기열을 거쳐 입장하세요.");
        }
        List<Screening> list = screenings.findByMovieIdOrderByBranchAscScreenNoAsc(movieId);
        if (list.isEmpty()) return List.of();

        List<String> ids = list.stream().map(Screening::getId).toList();
        Map<String, Long> soldById = new HashMap<>();
        for (Object[] row : bookingSeats.countGroupedByScreeningIds(ids)) {
            soldById.put((String) row[0], ((Number) row[1]).longValue());
        }

        // 임시점유도 판매완료와 같이 한 번에 받는다. 회차마다 부르면 회차 수만큼 왕복하고,
        // 이 화면은 게이트 뒤 첫 진입점이라 입장한 사람 전원이 반복해서 부른다.
        Map<String, Long> lockedById = locks.countLockedByScreening(ids);

        List<ScreeningView> out = new ArrayList<>(list.size());
        for (Screening s : list) {
            long sold = soldById.getOrDefault(s.getId(), 0L);       // 판매완료(영구, MySQL)
            long locked = lockedById.getOrDefault(s.getId(), 0L);   // 임시점유(Redis)
            // sold와 locked는 겹치지 않는다 — 확정 커밋 직후 그 좌석의 락을 해제하고, 판매된 좌석은
            // 다시 잠기지 않는다(select가 판매완료를 먼저 거른다). 커밋과 락 해제 사이 짧은 창에서만
            // 겹칠 수 있어 그때 잔여가 실제보다 작게 보이고, 다음 조회에서 복구된다.
            int remain = (int) Math.max(0, s.getTotalSeats() - sold - locked);
            out.add(new ScreeningView(s.getId(), s.getBranch(), s.getScreenNo(), s.getTotalSeats(), remain));
        }
        return out;
    }
}
