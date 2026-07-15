package com.cgv.booking;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

// booking 서비스 진입점. queue(Go)와 Kafka 두 토픽으로만 연결되는 예매 서비스.
// 영구 기록=MySQL(JPA·트랜잭션) / 임시 점유·입장인증=Redis. (설계 = 백엔드서비스-올인원 §3)
@EnableKafka
@SpringBootApplication
public class BookingApplication {
    public static void main(String[] args) {
        SpringApplication.run(BookingApplication.class, args);
    }
}
