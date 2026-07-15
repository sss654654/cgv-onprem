package com.cgv.booking.repo;

import com.cgv.booking.domain.BookingSeat;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface BookingSeatRepository extends JpaRepository<BookingSeat, Long> {
    // 판매완료 좌석(§3-1-2/3-1-8): 좌석도·잔여 계산의 "영구 판매" 출처.
    List<BookingSeat> findByScreeningId(String screeningId);
    long countByScreeningId(String screeningId);
    // 멱등 재사용 방어: 특정 예매의 좌석집합 — winner ↔ 요청 좌석 대조용.
    List<BookingSeat> findByBookingId(String bookingId);
}
