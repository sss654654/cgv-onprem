package com.cgv.booking.repo;

import com.cgv.booking.domain.Screening;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface ScreeningRepository extends JpaRepository<Screening, String> {
    // 회차 선택 화면(§3-1-1): 이 방송의 관 목록.
    List<Screening> findByMovieIdOrderByBranchAscScreenNoAsc(String movieId);
}
