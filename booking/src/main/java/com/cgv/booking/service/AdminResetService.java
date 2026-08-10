package com.cgv.booking.service;

import com.cgv.booking.config.CgvProps;
import com.cgv.booking.domain.Movie;
import com.cgv.booking.domain.Screening;
import com.cgv.booking.init.DataSeeder;
import com.cgv.booking.repo.BookingRepository;
import com.cgv.booking.repo.BookingSeatRepository;
import com.cgv.booking.repo.MovieRepository;
import com.cgv.booking.repo.ScreeningRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

// 데모 초기화 — 예매 기록·좌석 점유·입장 인증을 지우고 방송일을 다시 미래로 옮긴다.
//
// 필요한 이유: 예매는 영구 기록이라 좌석 4,000석이 한 번 팔리면 돌아오지 않는다.
// 가상 관객이 자동 예매를 돌수록 재고가 줄고, 바닥나면 모든 회차가 매진이라 데모가 멈춘다.
//
// 좌석·회차·영화 자체는 지우지 않는다 — 시드는 movies.count()==0일 때만 도는 1회성이라,
// 지우면 재시드 없이는 빈 화면이 된다. 지우는 것은 "쌓인 것"뿐이다.
@Service
public class AdminResetService {
    private static final Logger log = LoggerFactory.getLogger(AdminResetService.class);

    private final BookingRepository bookings;
    private final BookingSeatRepository bookingSeats;
    private final ScreeningRepository screenings;
    private final MovieRepository movies;
    private final StringRedisTemplate redis;
    private final CgvProps props;

    public AdminResetService(BookingRepository bookings, BookingSeatRepository bookingSeats,
                             ScreeningRepository screenings, MovieRepository movies,
                             StringRedisTemplate redis, CgvProps props) {
        this.bookings = bookings; this.bookingSeats = bookingSeats;
        this.screenings = screenings; this.movies = movies;
        this.redis = redis; this.props = props;
    }

    @Transactional
    public Map<String, Object> resetAll() {
        long seatRows = bookingSeats.count();
        long bookingRows = bookings.count();

        // 자식(booking_seats) 먼저 — bookings를 먼저 지우면 FK 위반.
        bookingSeats.deleteAllInBatch();
        bookings.deleteAllInBatch();

        int lockKeys = clearSeatLocks();
        int admittedKeys = clearAdmitted();

        movies.findById(props.getMovieId()).ifPresent(m -> {
            m.reschedule(DataSeeder.nextBroadcastAt());
            movies.save(m);
        });

        log.warn("초기화 실행 — 예매 {}건·좌석행 {}건 삭제, 좌석락 키 {}개·입장인증 키 {}개 정리",
                bookingRows, seatRows, lockKeys, admittedKeys);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("status", "RESET");
        out.put("bookingsDeleted", bookingRows);
        out.put("bookingSeatsDeleted", seatRows);
        out.put("seatLockKeysCleared", lockKeys);
        out.put("admittedKeysCleared", admittedKeys);
        movies.findById(props.getMovieId())
                .map(Movie::getBroadcastAt)
                .ifPresent(at -> out.put("broadcastAt", at.toString()));
        return out;
    }

    // 좌석 점유 정리 — 회차 목록으로 키를 계산한다(SCAN 불필요).
    // 인덱스 Set(seatlocks:{screeningId})의 멤버가 곧 잠긴 좌석번호이고, 좌석 키는 seat:{sc}:{seatNo}다.
    private int clearSeatLocks() {
        List<Screening> all = screenings.findAll();
        List<String> keys = new ArrayList<>();
        for (Screening sc : all) {
            String indexKey = "seatlocks:" + sc.getId();
            Set<String> members = redis.opsForSet().members(indexKey);
            if (members != null) {
                for (String seatNo : members) keys.add("seat:" + sc.getId() + ":" + seatNo);
            }
            keys.add(indexKey);
        }
        if (keys.isEmpty()) return 0;
        Long n = redis.delete(keys);
        return n == null ? 0 : n.intValue();
    }

    // 입장 인증 정리 — 키에 requestId가 들어가 목록을 알 수 없어 SCAN으로 훑는다.
    // 폴링 경로에서라면 쓰지 않을 방식이지만, 이 API는 사람이 수동으로 부르는 초기화라
    // 호출 빈도가 낮고 대상 키도 데모 규모(수백)다. 지우지 않으면 이미 발급된 인증이
    // TTL(기본 180초)까지 살아 있어 초기화 후에도 좌석 선택이 통과한다.
    private int clearAdmitted() {
        String pattern = "admitted:" + props.getMovieId() + ":*";
        List<String> keys = new ArrayList<>();
        try (Cursor<String> cursor = redis.scan(ScanOptions.scanOptions().match(pattern).count(500).build())) {
            while (cursor.hasNext()) keys.add(cursor.next());
        }
        if (keys.isEmpty()) return 0;
        Long n = redis.delete(keys);
        return n == null ? 0 : n.intValue();
    }
}
