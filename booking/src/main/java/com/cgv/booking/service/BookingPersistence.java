package com.cgv.booking.service;

import com.cgv.booking.domain.Booking;
import com.cgv.booking.domain.BookingSeat;
import com.cgv.booking.repo.BookingRepository;
import com.cgv.booking.repo.BookingSeatRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

// 확정의 DB 부분(§3-1-6) — 트랜잭션으로 묶는다.
// bookings + booking_seats를 한 단위로 INSERT → 절반 반영 방지.
// booking_seats UNIQUE(screening,seat) 위반 시 여기서 예외 → 트랜잭션 전체 롤백.
// (트랜잭션 프록시가 걸리도록 BookingService와 분리한 별도 빈.)
@Service
public class BookingPersistence {
    private final BookingRepository bookings;
    private final BookingSeatRepository bookingSeats;

    public BookingPersistence(BookingRepository bookings, BookingSeatRepository bookingSeats) {
        this.bookings = bookings; this.bookingSeats = bookingSeats;
    }

    @Transactional
    public void persist(Booking booking, String screeningId, List<String> seatNos) {
        bookings.save(booking);
        for (String seatNo : seatNos) {
            bookingSeats.save(new BookingSeat(booking.getId(), screeningId, seatNo));
            // ↑ UNIQUE(screening,seat) 위반이면 DataIntegrityViolationException → 롤백(booking도 취소)
        }
    }
}
