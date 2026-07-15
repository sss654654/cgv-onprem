package com.cgv.booking.domain;

import jakarta.persistence.*;

// 판매완료 좌석(booking_seats) — 예매당 여러 행. "판매완료" 판정의 진실원.
// UNIQUE(screening_id, seat_no) = DB 레벨 이중판매 최종 차단(§3-1-6).
//   Redis SET NX(1차)가 실패·만료해도, 같은 좌석 두 예매는 여기서 제약 위반→롤백→환불(보상).
@Entity
@Table(name = "booking_seats",
        uniqueConstraints = @UniqueConstraint(columnNames = {"screening_id", "seat_no"}))
public class BookingSeat {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "booking_id")
    private String bookingId;

    @Column(name = "screening_id")
    private String screeningId;

    @Column(name = "seat_no")
    private String seatNo;

    protected BookingSeat() {}
    public BookingSeat(String bookingId, String screeningId, String seatNo) {
        this.bookingId = bookingId; this.screeningId = screeningId; this.seatNo = seatNo;
    }
    public Long getId() { return id; }
    public String getBookingId() { return bookingId; }
    public String getScreeningId() { return screeningId; }
    public String getSeatNo() { return seatNo; }
}
