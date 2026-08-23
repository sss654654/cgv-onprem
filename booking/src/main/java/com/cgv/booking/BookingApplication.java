package com.cgv.booking;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

// booking 서비스 진입점. queue(Go)와 Kafka 두 토픽으로만 연결되는 예매 서비스.
// 영구 기록=MySQL(JPA·트랜잭션) / 임시 점유·입장인증=Redis.
// EnableScheduling: 좌석 재고 게이지(SeatInventoryMetrics)가 주기 갱신으로 값을 채운다.
@EnableKafka
@EnableScheduling
@SpringBootApplication
public class BookingApplication {
    public static void main(String[] args) {
        SpringApplication.run(BookingApplication.class, args);
    }
}
