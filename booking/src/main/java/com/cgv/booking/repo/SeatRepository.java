package com.cgv.booking.repo;

import com.cgv.booking.domain.Seat;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface SeatRepository extends JpaRepository<Seat, Long> {
    // 좌석도(§3-1-2): 그 관 좌석 구조 200행(단일 진실원).
    List<Seat> findByScreeningIdOrderBySeatRowAscSeatColAsc(String screeningId);
    long countByScreeningId(String screeningId);
}
