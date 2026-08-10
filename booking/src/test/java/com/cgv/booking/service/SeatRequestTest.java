package com.cgv.booking.service;

import com.cgv.booking.config.CgvProps;
import com.cgv.booking.web.ApiException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// 좌석 요청 정규화는 저장소를 건드리지 않는 순수 로직이라 실물 없이 검증된다.
// 정렬은 표시용이 아니다 — 좌석을 넣는 순서가 요청 순서를 따르면 [A1,A2]와 [A2,A1]이
// 동시에 들어올 때 서로가 잡은 행을 기다려 교착이 난다. 모든 요청이 같은 순서로 잡아야 한다.
class SeatRequestTest {

    // normalize는 개수 상한(설정)만 쓰고 저장소는 쓰지 않는다.
    private SeatRequest newSeatRequest(int maxSeats) {
        CgvProps props = new CgvProps();
        props.setMaxSeatsPerRequest(maxSeats);
        return new SeatRequest(null, null, props);
    }

    @Test
    @DisplayName("중복 좌석은 한 번만 남는다")
    void removesDuplicates() {
        List<String> result = newSeatRequest(8).normalize(List.of("A1", "A1", "B2"));
        assertEquals(List.of("A1", "B2"), result);
    }

    @Test
    @DisplayName("요청 순서와 무관하게 같은 순서로 정렬된다")
    void sortsRegardlessOfInputOrder() {
        SeatRequest req = newSeatRequest(8);
        assertEquals(req.normalize(List.of("A1", "A2")), req.normalize(List.of("A2", "A1")));
    }

    @Test
    @DisplayName("개수 상한을 넘으면 거절한다")
    void rejectsTooManySeats() {
        SeatRequest req = newSeatRequest(2);
        assertThrows(ApiException.class, () -> req.normalize(List.of("A1", "A2", "A3")));
    }

    @Test
    @DisplayName("빈 요청은 거절한다")
    void rejectsEmptyRequest() {
        SeatRequest req = newSeatRequest(8);
        assertThrows(ApiException.class, () -> req.normalize(List.of()));
        assertThrows(ApiException.class, () -> req.normalize(null));
    }
}
