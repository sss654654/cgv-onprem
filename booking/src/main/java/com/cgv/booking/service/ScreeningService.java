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
import java.util.concurrent.ConcurrentHashMap;

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
        return compute(movieId);
    }

    // 좌석 현황판 — 입장 전에도 보이는 집계. 게이트가 없다.
    // 좌석 번호나 누가 잡았는지는 내보내지 않고 회차별 총/잔여 수만 준다. 실제 예매는 여전히
    //   게이트 뒤에서만 되고, 이 숫자는 매표소 앞 전광판처럼 밖에서 보라고 있는 값이다.
    //
    // ★ 이 경로만 결과를 짧게 캐시한다. 게이트 뒤 목록(listForMovie)과 달리 누구나 부를 수 있고,
    //   부르는 화면이 영화 목록 — 오픈을 기다리는 인원 전부가 앉아 있는 자리다. 방문자마다
    //   MySQL group by 와 Redis 다중 조회가 돌면 대기 인원에 비례해 그 부하가 곱해진다.
    //   전광판이므로 초 단위로 최신일 필요가 없다. 같은 창 안의 요청은 한 번 읽은 값을 나눠 쓴다.
    private static final long BOARD_TTL_MS = 1000;
    private final Map<String, Cached> boardCache = new ConcurrentHashMap<>();
    private record Cached(long at, List<ScreeningView> views) {}

    public List<ScreeningView> board(String movieId) {
        long now = System.currentTimeMillis();
        Cached c = boardCache.get(movieId);
        if (c != null && now - c.at() < BOARD_TTL_MS) return c.views();
        List<ScreeningView> views = compute(movieId);
        boardCache.put(movieId, new Cached(now, views));
        return views;
    }

    private List<ScreeningView> compute(String movieId) {
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
