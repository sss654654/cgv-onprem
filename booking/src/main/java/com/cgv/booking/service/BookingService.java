package com.cgv.booking.service;

import com.cgv.booking.config.CgvProps;
import com.cgv.booking.domain.Booking;
import com.cgv.booking.domain.BookingSeat;
import com.cgv.booking.domain.Screening;
import com.cgv.booking.kafka.CompletedProducer;
import com.cgv.booking.redis.AdmittedService;
import com.cgv.booking.redis.SeatLockService;
import com.cgv.booking.repo.BookingRepository;
import com.cgv.booking.repo.BookingSeatRepository;
import com.cgv.booking.repo.ScreeningRepository;
import com.cgv.booking.web.ApiException;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

// 결제 → 확정 → 완료(§3-1-5/6/7). "결제하기" 한 요청의 3국면.
// 트랜잭션(DB)은 BookingPersistence, 외부 PG·후처리는 여기서 오케스트레이션.
@Service
public class BookingService {
    private static final Logger log = LoggerFactory.getLogger(BookingService.class);
    private final ScreeningRepository screenings;
    private final BookingRepository bookings;
    private final BookingSeatRepository bookingSeats;
    private final BookingPersistence persistence;
    private final SeatLockService locks;
    private final AdmittedService admitted;
    private final PaymentGateway pg;
    private final CompletedProducer completed;
    private final CgvProps props;
    private final MeterRegistry meterRegistry;

    public BookingService(ScreeningRepository screenings, BookingRepository bookings, BookingSeatRepository bookingSeats,
                          BookingPersistence persistence, SeatLockService locks, AdmittedService admitted, PaymentGateway pg,
                          CompletedProducer completed, CgvProps props, MeterRegistry meterRegistry) {
        this.screenings = screenings; this.bookings = bookings; this.bookingSeats = bookingSeats; this.persistence = persistence;
        this.locks = locks; this.admitted = admitted; this.pg = pg; this.completed = completed; this.props = props;
        this.meterRegistry = meterRegistry;
    }

    // 관측(§7-B 도메인 지표): booking_confirm_total{result=...} — confirm() 종착점별 카운트.
    // 순수 계측(ADD) — 결제/트랜잭션/환불 로직은 건드리지 않는다.
    private void countConfirm(String result) {
        meterRegistry.counter("booking.confirm", "result", result).increment();
    }

    public record Result(String bookingId, String screeningId, List<String> seatNos, int price) {}

    public Result confirm(String screeningId, List<String> seatNos, String requestId, String idempotencyKey) {
        Screening sc = screenings.findById(screeningId)
                .orElseThrow(() -> {
                    countConfirm("no_screening");
                    return new ApiException(org.springframework.http.HttpStatus.NOT_FOUND, "NO_SCREENING", "회차 없음");
                });
        String movieId = sc.getMovieId();

        // ① 게이트(§3-1-5)
        if (!admitted.isAdmitted(movieId, requestId)) {
            countConfirm("forbidden");
            throw ApiException.forbidden("입장객이 아닙니다.");
        }

        // ② 멱등(§3-1-5): 같은 결제 두 번 → 기존 결과 반환(재결제 안 함).
        var existing = bookings.findByIdempotencyKey(idempotencyKey);
        if (existing.isPresent()) {
            Booking b = existing.get();
            ensureSameRequest(b, screeningId, seatNos);   // 멱등키 재사용 방어: 다른 예매면 409 거절
            countConfirm("replay");
            return new Result(b.getId(), b.getScreeningId(), seatNos, b.getPrice());
        }

        // ③ 락 재확인 + 갱신(§2-D [7]): 결제 전 내 좌석 살아있으면 TTL 연장(PG 도는 사이 만료 방지).
        if (!locks.renewMine(screeningId, seatNos, requestId)) {
            countConfirm("lock_expired");
            throw ApiException.conflict("좌석 점유가 만료되었거나 내 좌석이 아닙니다. 다시 선택하세요.");
        }

        int price = seatNos.size() * props.getPricePerSeat();

        // ④ 결제(PG 동기, §3-1-5). 승인 후 돈 빠짐.
        String paymentId = pg.approve(idempotencyKey, price);

        // ⑤ 확정(트랜잭션, §3-1-6). 커밋 실패 → 롤백 → 보상(환불).
        // UNIQUE 위반(좌석 선점 경합)만이 아니라 어떤 커밋 실패든 돈은 돌려준다
        // (커넥션 단절·데드락 등에서 "돈 빠지고 예매·환불 둘 다 없음" 경로 차단).
        String bookingId = "BK-" + UUID.randomUUID().toString().substring(0, 8);
        try {
            persistence.persist(new Booking(bookingId, screeningId, requestId, price, idempotencyKey), screeningId, seatNos);
        } catch (DataIntegrityViolationException dup) {
            // 멱등키 동시요청 방어(§2-E): 같은 멱등키가 동시에 오면 R2가 여기서 UNIQUE 위반.
            // paymentId가 멱등키 공유(mock "PAY-"+key)라 여기서 환불하면 이미 성공한 R1의 결제를 취소하는 버그.
            // → 멱등키로 재조회해 "먼저 성공한 예매"가 있으면 반환(환불 금지). 없으면 좌석 UNIQUE 위반 = 실제 경합.
            var winner = bookings.findByIdempotencyKey(idempotencyKey);
            if (winner.isPresent()) {
                Booking b = winner.get();
                ensureSameRequest(b, screeningId, seatNos);   // 같은 요청의 동시 중복만 성공 반환. 다르면(키 재사용·좌석위반 오분류) 409
                countConfirm("replay");
                return new Result(b.getId(), b.getScreeningId(), seatNos, b.getPrice());
            }
            countConfirm("seat_conflict");
            safeRefund(paymentId);   // 좌석 선점 경합 → 돈 빠졌는데 좌석 못 줌 → 환불(보상)
            throw ApiException.conflict("이미 판매된 좌석이 있습니다. 결제는 자동 취소됩니다.");
        } catch (RuntimeException e) {
            countConfirm("error");
            safeRefund(paymentId);   // 기타 커밋 실패(DB 단절·데드락)도 보상 후 전파(§3-1-6)
            throw e;
        }

        // ⑥ 완료(§3-1-7, 커밋 후 후처리): 락 해제 + queue에 자리반납 + 인증 소진.
        // 예매는 이미 커밋됨 — 후처리 실패가 클라에 500으로 새면 "성공을 실패로 오인".
        // 각 단계는 실패해도 로그만 남기고 계속(락=TTL 자연회수 / 자리반납=세션 타임아웃이 최후 회수).
        try {
            locks.releaseMine(screeningId, seatNos, requestId);
        } catch (RuntimeException e) {
            log.warn("커밋 후 락 해제 실패(TTL로 자연 회수): screening={} req={} err={}", screeningId, requestId, e.getMessage());
        }
        try {
            completed.publishCompleted(requestId, movieId);    // → queue ZREM active (닫힌 순환)
        } catch (RuntimeException e) {
            log.error("완료 이벤트 발행 실패 — queue 자리 반납 지연(세션 타임아웃이 회수): req={} err={}", requestId, e.getMessage());
        }
        try {
            admitted.remove(movieId, requestId);               // 인증 소진(재입장 전 재예매 불가)
        } catch (RuntimeException e) {
            log.warn("admitted 소진 실패: movie={} req={} err={}", movieId, requestId, e.getMessage());
        }

        countConfirm("success");
        return new Result(bookingId, screeningId, seatNos, price);
    }

    // 멱등 재사용 방어: winner가 현재 요청과 "같은 예매"(회차+좌석집합 일치)인지 확인.
    // 다르면 = 같은 멱등키를 다른 요청에 재사용 → winner id·price에 요청 좌석이 섞이는 오정보/좌석위반 오분류를 막고 409 거절.
    // (정상 사용=더블클릭이면 좌석이 항상 일치해 통과. 클라이언트 계약 위반 방어.)
    private void ensureSameRequest(Booking winner, String screeningId, List<String> seatNos) {
        Set<String> won = bookingSeats.findByBookingId(winner.getId()).stream()
                .map(BookingSeat::getSeatNo).collect(Collectors.toSet());
        if (!winner.getScreeningId().equals(screeningId) || !won.equals(new HashSet<>(seatNos))) {
            throw ApiException.conflict("동일 멱등키로 다른 예매 요청이 접수되었습니다. 새로 시도하세요.");
        }
    }

    // 환불 예외 보호(§0-E): refund 실패를 유실하지 않는다(돈). 실 PG면 "돈 빠지고 환불도 유실"이
    // 로그 없이 새는 걸 막음 — error 로그로 남겨 대사(reconciliation) 대상으로.
    private void safeRefund(String paymentId) {
        try {
            pg.refund(paymentId);
        } catch (RuntimeException e) {
            log.error("★ 환불 실패 — 수동 대사 필요! paymentId={} err={}", paymentId, e.getMessage(), e);
        }
    }
}
