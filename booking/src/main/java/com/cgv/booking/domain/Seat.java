package com.cgv.booking.domain;

import jakarta.persistence.*;

// 좌석(seat) = 회차의 좌석 구조(단일 진실원). 화면 격자는 이걸 그대로 렌더 → "200인데 48칸" 불가(§3-1-8).
// status 컬럼 없음 — "판매완료"는 booking_seats 행 존재로 판정(§3-1-8). 여기는 구조만.
@Entity
@Table(name = "seats",
        uniqueConstraints = @UniqueConstraint(columnNames = {"screening_id", "seat_no"}))
public class Seat {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "screening_id")
    private String screeningId;

    @Column(name = "seat_row")
    private int seatRow;            // 0~9 (A~J)

    @Column(name = "seat_col")
    private int seatCol;            // 1~20

    @Column(name = "seat_no")
    private String seatNo;          // 예: A4 (행문자+열번호)

    protected Seat() {}
    public Seat(String screeningId, int seatRow, int seatCol, String seatNo) {
        this.screeningId = screeningId; this.seatRow = seatRow; this.seatCol = seatCol; this.seatNo = seatNo;
    }
    public Long getId() { return id; }
    public String getScreeningId() { return screeningId; }
    public int getSeatRow() { return seatRow; }
    public int getSeatCol() { return seatCol; }
    public String getSeatNo() { return seatNo; }
}
