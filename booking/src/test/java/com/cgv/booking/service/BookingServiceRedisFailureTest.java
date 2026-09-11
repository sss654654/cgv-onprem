package com.cgv.booking.service;

import com.cgv.booking.config.CgvProps;
import com.cgv.booking.domain.Screening;
import com.cgv.booking.kafka.CompletedProducer;
import com.cgv.booking.redis.AdmittedService;
import com.cgv.booking.redis.SeatLockService;
import com.cgv.booking.repo.BookingRepository;
import com.cgv.booking.repo.BookingSeatRepository;
import com.cgv.booking.repo.ScreeningRepository;
import com.cgv.booking.web.ApiException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.http.HttpStatus;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

// Redis 가 느리거나 끊겼을 때 예매 확정 요청이 어떻게 끝나는지.
// 게이트 · 락 확인은 결제 전이라 돈이 움직이지 않아야 하고, 실패가 지표(result=redis_error)에 남아야 한다.
// 저장소 · Redis · PG 는 전부 가짜로 두고 BookingService 의 분기만 본다.
class BookingServiceRedisFailureTest {
    private final ScreeningRepository screenings = mock(ScreeningRepository.class);
    private final BookingRepository bookings = mock(BookingRepository.class);
    private final AdmittedService admitted = mock(AdmittedService.class);
    private final SeatLockService locks = mock(SeatLockService.class);
    private final SeatRequest seatRequest = mock(SeatRequest.class);
    private final PaymentGateway pg = mock(PaymentGateway.class);
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private BookingService service;

    @BeforeEach
    void setUp() {
        Screening screening = mock(Screening.class);
        when(screening.getMovieId()).thenReturn("M1");
        when(screenings.findById("S1")).thenReturn(Optional.of(screening));
        when(bookings.findByIdempotencyKey("K1")).thenReturn(Optional.empty());
        when(seatRequest.normalize(anyList())).thenReturn(List.of("A1"));
        service = new BookingService(screenings, bookings, mock(BookingSeatRepository.class),
                mock(BookingPersistence.class), locks, admitted, seatRequest, pg,
                mock(CompletedProducer.class), new CgvProps(), meters);
    }

    private double redisErrors() {
        Counter c = meters.find("booking.confirm").tag("result", "redis_error").counter();
        return c == null ? 0 : c.count();
    }

    @Test
    @DisplayName("입장 인증 확인에서 Redis 가 시간 초과면 503 · redis_error 로 끝나고 결제하지 않는다")
    void gateTimeout() {
        when(admitted.isAdmitted("M1", "R1")).thenThrow(new QueryTimeoutException("command timed out"));

        ApiException e = assertThrows(ApiException.class,
                () -> service.confirm("S1", List.of("A1"), "R1", "K1"));

        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, e.getStatus());
        assertEquals(1, redisErrors());
        verifyNoInteractions(pg);
    }

    @Test
    @DisplayName("좌석 락 연장에서 Redis 연결이 끊기면 503 · redis_error 로 끝나고 결제하지 않는다")
    void renewConnectionFailure() {
        when(admitted.isAdmitted("M1", "R1")).thenReturn(true);
        when(locks.renewMine(eq("S1"), anyList(), eq("R1")))
                .thenThrow(new RedisConnectionFailureException("connection refused"));

        ApiException e = assertThrows(ApiException.class,
                () -> service.confirm("S1", List.of("A1"), "R1", "K1"));

        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, e.getStatus());
        assertEquals(1, redisErrors());
        verifyNoInteractions(pg);
    }
}
