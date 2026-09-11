package com.cgv.booking.migration;

import com.cgv.booking.config.CgvProps;
import com.cgv.booking.domain.Movie;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;

// V2 — 데모 시드: 영화 1 · 회차 20(지점 5 × 관 4) · 좌석 4,000(관당 A-J × 1-20 = 200).
//
// SQL 이 아니라 Java 인 이유: 방송일을 "적용 시점 +7일 18:00" 으로 계산하고(초기화 API 와 같은 계산),
//   지점 · 관 · 좌석 격자를 application.yml 의 cgv.* 값으로 만든다.
// Flyway 가 DB 잠금 아래에서 한 번만 돌리고 이력에 남기므로, 파드가 여러 대여도 한 번만 들어간다.
//   (전에는 파드마다 "비어 있으면 넣기" 를 돌려 두 대가 동시에 뜨면 PK 가 겹쳤다.)
// movies 에 행이 있으면 넣지 않는다 — Flyway 도입 전에 시드가 들어간 DB 는 V1 을 baseline 으로 기록한 뒤
//   이 마이그레이션을 만나는데, 그대로 넣으면 PK 가 겹친다.
// Spring 빈이라 Spring Boot 가 Flyway 에 등록한다. 버전(2)과 설명은 클래스 이름에서 온다.
// JPA 보다 먼저 돌므로 리포지토리를 못 쓴다 — Flyway 가 주는 커넥션으로 JDBC 를 직접 쓴다.
@Component
public class V2__SeedDemoData extends BaseJavaMigration {
    private static final Logger log = LoggerFactory.getLogger(V2__SeedDemoData.class);
    private final CgvProps props;

    public V2__SeedDemoData(CgvProps props) {
        this.props = props;
    }

    @Override
    public void migrate(Context context) throws Exception {
        Connection c = context.getConnection();

        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM movies")) {
            rs.next();
            long existing = rs.getLong(1);
            if (existing > 0) {
                log.info("시드 생략 — movies 에 이미 {}행", existing);
                return;
            }
        }

        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO movies (id, title, broadcast_at) VALUES (?, ?, ?)")) {
            ps.setString(1, props.getMovieId());
            ps.setString(2, props.getMovieTitle());
            ps.setTimestamp(3, Timestamp.valueOf(Movie.nextBroadcastAt()));
            ps.executeUpdate();
        }

        int rows = props.getRows(), cols = props.getCols();
        int perScreen = rows * cols;   // 200
        int scIdx = 0, seatCount = 0;
        try (PreparedStatement scr = c.prepareStatement(
                     "INSERT INTO screenings (id, movie_id, branch, screen_no, total_seats) VALUES (?, ?, ?, ?, ?)");
             PreparedStatement seat = c.prepareStatement(
                     "INSERT INTO seats (screening_id, seat_row, seat_col, seat_no) VALUES (?, ?, ?, ?)")) {
            for (String branch : props.branchList()) {
                for (int screenNo = 1; screenNo <= props.getScreensPerBranch(); screenNo++) {
                    scIdx++;
                    String screeningId = "sc" + scIdx;   // sc1..sc20 (ASCII — Redis 키에 그대로 들어간다)
                    scr.setString(1, screeningId);
                    scr.setString(2, props.getMovieId());
                    scr.setString(3, branch.trim());
                    scr.setInt(4, screenNo);
                    scr.setInt(5, perScreen);
                    scr.addBatch();
                    for (int r = 0; r < rows; r++) {
                        char rowChar = (char) ('A' + r);   // A-J
                        for (int col = 1; col <= cols; col++) {
                            seat.setString(1, screeningId);
                            seat.setInt(2, r);
                            seat.setInt(3, col);
                            seat.setString(4, "" + rowChar + col);   // A1..J20
                            seat.addBatch();
                            seatCount++;
                        }
                    }
                }
            }
            scr.executeBatch();
            seat.executeBatch();
        }
        log.info("시드 완료: 영화 1 · 회차 {} · 좌석 {}", scIdx, seatCount);
    }
}
