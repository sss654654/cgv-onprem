package com.cgv.booking.metrics;

import com.cgv.booking.repo.BookingSeatRepository;
import com.cgv.booking.repo.SeatRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

// 좌석 재고를 실제 행 수로 내보낸다.
//
// 이 값을 쓰던 mysql_info_schema_table_rows 는 InnoDB 가 페이지 20개(innodb_stats_persistent_sample_pages)
//   를 표본으로 잡은 추정치다. 대량 삭제 뒤에는 purge 가 끝나기 전이라 삭제 표시만 된 레코드가
//   표본에 그대로 들어와, 통계를 재계산해도 삭제 전 값이 나온다 — 2026-08-23 실측에서 실제 0 행인
//   booking_seats 를 3,529 로 보고했다. 재고가 바닥나면 부하 판이 끝나므로 그 판정을 추정치로 할 수 없다.
//
// 스크레이프마다 세지 않고 주기 갱신한 값을 읽는다. 세는 질의 수가 스크레이프 주기·수와 무관해지고,
//   지표 수집이 DB 응답을 기다리지 않는다(게이지 공급자에서 질의하면 DB 가 느릴 때 스크레이프가 같이 늦는다).
@Component
public class SeatInventoryMetrics {

    private static final Logger log = LoggerFactory.getLogger(SeatInventoryMetrics.class);

    private final SeatRepository seats;
    private final BookingSeatRepository bookingSeats;

    // 게이지가 읽어 가는 자리. Micrometer 는 등록한 Number 를 약한 참조로 들고 있어,
    //   싱글턴 빈의 필드로 둬야 수집이 끊기지 않는다.
    private final AtomicLong remaining = new AtomicLong();
    private final AtomicLong sold = new AtomicLong();

    public SeatInventoryMetrics(SeatRepository seats, BookingSeatRepository bookingSeats,
                                MeterRegistry meterRegistry) {
        this.seats = seats;
        this.bookingSeats = bookingSeats;
        meterRegistry.gauge("booking.seats.remaining", remaining);
        meterRegistry.gauge("booking.seats.sold", sold);
    }

    // fixedDelay 는 앞 실행이 끝난 뒤부터 센다 — 질의가 늦어져도 호출이 겹치지 않는다.
    // 첫 실행은 컨텍스트 기동 직후라 게이지가 0 으로 보이는 구간은 밀리초 단위다.
    @Scheduled(fixedDelay = 5_000)
    public void refresh() {
        try {
            long total = seats.count();
            long soldNow = bookingSeats.count();
            remaining.set(total - soldNow);
            sold.set(soldNow);
        } catch (RuntimeException e) {
            // 직전 값을 그대로 둔다. 0 으로 떨어뜨리면 재고 소진과 구분이 안 된다.
            log.warn("좌석 재고 집계 실패 — 직전 값을 유지한다", e);
        }
    }
}
