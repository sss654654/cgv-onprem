package com.cgv.booking.repo;

import com.cgv.booking.domain.Seat;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Collection;
import java.util.List;

public interface SeatRepository extends JpaRepository<Seat, Long> {
    // 좌석도: 그 관 좌석 구조 200행(단일 진실원).
    List<Seat> findByScreeningIdOrderBySeatRowAscSeatColAsc(String screeningId);
    long countByScreeningId(String screeningId);

    // 요청한 좌석번호 중 그 회차에 실재하는 것만 반환. 요청 개수와 다르면 없는 좌석이 섞인 것.
    // booking_seats에는 seats로의 FK가 없어, 실재 검증을 안 하면
    // 존재하지 않는 좌석번호로 점유·결제·확정이 전부 통과하고 그 행이 잔여좌석을 깎는다.
    @Query("select s.seatNo from Seat s where s.screeningId = :screeningId and s.seatNo in :seatNos")
    List<String> findExistingSeatNos(@Param("screeningId") String screeningId, @Param("seatNos") Collection<String> seatNos);
}
