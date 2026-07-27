package com.cgv.booking.service;

import com.cgv.booking.domain.Booking;
import com.cgv.booking.domain.BookingSeat;
import com.cgv.booking.repo.BookingRepository;
import com.cgv.booking.repo.BookingSeatRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
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

    // timeout 5초: 정상 경로는 INSERT 몇 건이라 밀리초 단위다. 5초를 넘었다면 다른 트랜잭션의 행 락을
    // 기다리고 있다는 뜻이고, 상한이 없으면 innodb_lock_wait_timeout(기본 50초)까지 커넥션 1개를 붙잡는다.
    // 풀이 10개면 그런 요청 10개로 앱 전체가 멈춘다. 여기서 끊고 호출부가 환불·409로 마무리한다.
    @Transactional(timeout = 5)
    public void persist(Booking booking, String screeningId, List<String> seatNos) {
        bookings.save(booking);
        // 좌석 INSERT 순서를 요청 순서가 아니라 항상 정렬 순서로 고정한다.
        // [A1,A2]와 [A2,A1]이 동시에 들어오면 서로 상대가 잡은 행을 기다려 InnoDB 데드락(1213)이 된다.
        // 모든 트랜잭션이 같은 순서로 잡으면 순환 대기가 만들어지지 않는다.
        // (호출부가 이미 정렬해 넘기지만, 이 트랜잭션 단독으로도 성립해야 하는 불변식이라 여기서 다시 고정한다.)
        List<String> ordered = new ArrayList<>(seatNos);
        ordered.sort(String::compareTo);
        for (String seatNo : ordered) {
            bookingSeats.save(new BookingSeat(booking.getId(), screeningId, seatNo));
            // ↑ UNIQUE(screening,seat) 위반이면 DataIntegrityViolationException → 롤백(booking도 취소)
        }
    }
}
