package com.cgv.booking.repo;

import com.cgv.booking.domain.Booking;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;

public interface BookingRepository extends JpaRepository<Booking, String> {
    // 멱등(§3-1-5): 같은 결제 두 번 오면 기존 결과 반환.
    Optional<Booking> findByIdempotencyKey(String idempotencyKey);
}
