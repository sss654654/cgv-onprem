package com.cgv.booking.init;

import com.cgv.booking.config.CgvProps;
import com.cgv.booking.domain.Movie;
import com.cgv.booking.domain.Screening;
import com.cgv.booking.domain.Seat;
import com.cgv.booking.repo.MovieRepository;
import com.cgv.booking.repo.ScreeningRepository;
import com.cgv.booking.repo.SeatRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

// 도메인 픽스값 시드(§3-1-8): 영화1 · 회차20(지점5×관4) · 좌석4,000(관당 A~J×1~20=200).
// 멀티팟 레이스 차단(§2-E): 기본 비활성. 스키마·시드는 배포 Job(플랫폼 B)이 단일 실행한다.
//   count()==0 가드가 비원자(TOCTOU)라 M파드 동시 기동 시 PK 충돌 → CrashLoopBackOff.
//   로컬 단일 파드 개발 시에만 SEED_ON_START=true(+ DDL_AUTO=update)로 켠다.
@Component
@ConditionalOnProperty(name = "cgv.seed-on-start", havingValue = "true")
public class DataSeeder implements CommandLineRunner {
    private static final Logger log = LoggerFactory.getLogger(DataSeeder.class);
    private final MovieRepository movies;
    private final ScreeningRepository screenings;
    private final SeatRepository seats;
    private final CgvProps props;

    public DataSeeder(MovieRepository movies, ScreeningRepository screenings, SeatRepository seats, CgvProps props) {
        this.movies = movies; this.screenings = screenings; this.seats = seats; this.props = props;
    }

    @Override
    public void run(String... args) {
        if (movies.count() > 0) {
            log.info("시드 생략(이미 존재). movies={}", movies.count());
            return;
        }
        // 영화(방송) 1 — 18:00 단일 시각.
        LocalDateTime at = LocalDate.now().atTime(18, 0);
        movies.save(new Movie(props.getMovieId(), props.getMovieTitle(), at));

        String[] branches = props.branchList();
        int perBranch = props.getScreensPerBranch();
        int rows = props.getRows(), cols = props.getCols();
        int total = rows * cols;   // 200

        List<Seat> seatBatch = new ArrayList<>();
        int scIdx = 0;
        for (String branch : branches) {
            for (int screenNo = 1; screenNo <= perBranch; screenNo++) {
                scIdx++;
                String screeningId = "sc" + scIdx;     // sc1..sc20 (ASCII, Redis 키 안전)
                screenings.save(new Screening(screeningId, props.getMovieId(), branch.trim(), screenNo, total));
                for (int r = 0; r < rows; r++) {
                    char rowChar = (char) ('A' + r);    // A~J
                    for (int col = 1; col <= cols; col++) {
                        String seatNo = "" + rowChar + col;  // A1..J20
                        seatBatch.add(new Seat(screeningId, r, col, seatNo));
                    }
                }
            }
        }
        seats.saveAll(seatBatch);
        log.info("시드 완료: 영화1 · 회차{} · 좌석{}", scIdx, seatBatch.size());
    }
}
