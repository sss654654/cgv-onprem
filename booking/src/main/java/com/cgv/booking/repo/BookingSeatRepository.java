package com.cgv.booking.repo;

import com.cgv.booking.domain.BookingSeat;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Collection;
import java.util.List;

public interface BookingSeatRepository extends JpaRepository<BookingSeat, Long> {
    // 판매완료 좌석: 좌석도·잔여 계산의 "영구 판매" 출처.
    List<BookingSeat> findByScreeningId(String screeningId);
    long countByScreeningId(String screeningId);
    // 멱등 재사용 방어: 특정 예매의 좌석집합 — winner ↔ 요청 좌석 대조용.
    List<BookingSeat> findByBookingId(String bookingId);

    // 회차 목록 화면: 회차마다 count 쿼리를 돌리면 회차 수만큼 왕복이 생긴다(회차 20 = 20왕복).
    // 한 번의 group by로 (회차, 판매수)를 전부 받아 왕복을 1회로 만든다.
    // 반환 행 = [screeningId, count]. 판매가 0인 회차는 행이 없다(호출부가 0으로 채운다).
    @Query("select bs.screeningId, count(bs) from BookingSeat bs where bs.screeningId in :screeningIds group by bs.screeningId")
    List<Object[]> countGroupedByScreeningIds(@Param("screeningIds") Collection<String> screeningIds);

    // 이미 판매된 좌석인지 확인(요청한 좌석 중 팔린 것만 반환).
    // 좌석락은 확정 직후 해제되므로 Redis에는 "판매완료"의 흔적이 없다 → 점유·결제 전에 여기서 확인해야
    // 이미 팔린 좌석을 다시 잠그고 PG 승인까지 가는 경로가 막힌다.
    @Query("select bs.seatNo from BookingSeat bs where bs.screeningId = :screeningId and bs.seatNo in :seatNos")
    List<String> findSoldSeatNos(@Param("screeningId") String screeningId, @Param("seatNos") Collection<String> seatNos);
}
