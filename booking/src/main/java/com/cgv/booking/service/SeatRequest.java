package com.cgv.booking.service;

import com.cgv.booking.config.CgvProps;
import com.cgv.booking.repo.BookingSeatRepository;
import com.cgv.booking.repo.SeatRepository;
import com.cgv.booking.web.ApiException;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

// 좌석 요청(seatNos)의 정규화·검증. 점유(select)와 확정(confirm)이 같은 규칙을 쓴다.
//
// 정규화 = 중복 제거 + 정렬.
//   중복 제거: ["A1","A1"]이면 2석 값을 청구하고 INSERT는 UNIQUE에 걸려 롤백·환불로 끝난다.
//   정렬: 좌석 INSERT 순서가 요청 순서를 따르면 [A1,A2]와 [A2,A1]이 동시에 들어올 때 서로를 기다려
//         InnoDB 데드락이 난다. 모든 요청이 같은 순서로 잡으면 순환 대기가 생기지 않는다.
//
// 검증 = 개수 상한 → 실재 → 판매완료 순서. 싼 검사부터 해야 Redis·DB를 덜 건드린다.
@Service
public class SeatRequest {
    private final SeatRepository seats;
    private final BookingSeatRepository bookingSeats;
    private final CgvProps props;

    public SeatRequest(SeatRepository seats, BookingSeatRepository bookingSeats, CgvProps props) {
        this.seats = seats; this.bookingSeats = bookingSeats; this.props = props;
    }

    // 개수 상한만 검사하고 정규화한다(외부 I/O 없음). 실재·판매 검사 전에 먼저 불러 큰 요청을 잘라낸다.
    public List<String> normalize(List<String> seatNos) {
        if (seatNos == null || seatNos.isEmpty()) {
            throw ApiException.badRequest("EMPTY_SEATS", "좌석을 선택하세요.");
        }
        int max = props.getMaxSeatsPerRequest();
        if (seatNos.size() > max) {
            // 상한이 없으면 좌석 수만 개를 담은 요청 하나가 Lua 스크립트 한 번으로 단일 스레드 Redis를
            // 그 실행 시간 동안 통째로 멈춘다(queue 폴링과 같은 인스턴스).
            throw ApiException.badRequest("TOO_MANY_SEATS", "한 번에 최대 " + max + "석까지 선택할 수 있습니다.");
        }
        Set<String> uniq = new TreeSet<>();
        for (String s : seatNos) {
            if (s == null || s.isBlank()) throw ApiException.badRequest("BAD_SEAT_NO", "좌석번호가 비어 있습니다.");
            uniq.add(s.trim());
        }
        return new ArrayList<>(uniq);
    }

    // 그 회차에 실재하는 좌석인지. seats 테이블이 좌석 구조의 단일 진실원이다.
    public void ensureSeatsExist(String screeningId, List<String> seatNos) {
        Set<String> found = new HashSet<>(seats.findExistingSeatNos(screeningId, seatNos));
        if (found.size() != seatNos.size()) {
            List<String> unknown = new ArrayList<>();
            for (String s : seatNos) if (!found.contains(s)) unknown.add(s);
            throw ApiException.badRequest("NO_SUCH_SEAT", "존재하지 않는 좌석입니다: " + String.join(",", unknown));
        }
    }

    // 이미 판매된 좌석인지. 확정 직후 좌석락이 지워지므로 Redis만 봐서는 판매완료를 알 수 없다.
    public void ensureNotSold(String screeningId, List<String> seatNos) {
        List<String> sold = bookingSeats.findSoldSeatNos(screeningId, seatNos);
        if (!sold.isEmpty()) {
            throw ApiException.conflict("이미 판매된 좌석입니다: " + String.join(",", sold));
        }
    }
}
