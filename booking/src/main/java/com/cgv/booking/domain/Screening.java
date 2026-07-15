package com.cgv.booking.domain;

import jakarta.persistence.*;

// 회차(screening) = 지점 × 관 (생중계라 시각은 movie 단위 18:00 고정). 좌석 재고의 단위.
// 사용자는 입장(admitted) 후 이 회차(관)를 고르고 그 회차 좌석을 잡는다(§3-1-1).
@Entity
@Table(name = "screenings")
public class Screening {
    @Id
    private String id;            // 예: yongsan-1  (= seat 키 seat:{screeningId}:{seatNo}의 단위)
    private String movieId;       // 어느 방송
    private String branch;        // 지점명 (용산아이파크몰 ...)
    private int screenNo;         // 관 번호 1~4
    private int totalSeats;       // 200 (관당 균일)

    protected Screening() {}
    public Screening(String id, String movieId, String branch, int screenNo, int totalSeats) {
        this.id = id; this.movieId = movieId; this.branch = branch;
        this.screenNo = screenNo; this.totalSeats = totalSeats;
    }
    public String getId() { return id; }
    public String getMovieId() { return movieId; }
    public String getBranch() { return branch; }
    public int getScreenNo() { return screenNo; }
    public int getTotalSeats() { return totalSeats; }
}
