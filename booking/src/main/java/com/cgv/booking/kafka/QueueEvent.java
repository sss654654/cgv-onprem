package com.cgv.booking.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

// queue ↔ booking 공용 메시지 형식 (§2-4, §3-1-3).
//   admissions          queue→booking : 입장 승인  {requestId, movieId}
//   admissions-revoked  queue→booking : 입장 회수  {requestId, movieId, reason}
//   bookings-completed  booking→queue : 자리 반환  {requestId, movieId}
//
// reason은 회수 이벤트에만 실린다(SESSION_TIMEOUT·LEAVE·COMPLETE). 관측·디버깅용이고
// 처리 분기에는 쓰지 않는다 — 어떤 사유든 하는 일은 "입장 인증 삭제" 하나다.
//
// ignoreUnknown: 발행 측이 필드를 추가해도 소비가 안 깨지게 한다. 두 서비스가 따로 배포되므로
// 한쪽만 먼저 올라간 구간이 항상 생긴다.
@JsonIgnoreProperties(ignoreUnknown = true)
public record QueueEvent(String requestId, String movieId, String reason) {

    // reason이 없는 이벤트(admissions·bookings-completed)를 만들 때 쓴다.
    public QueueEvent(String requestId, String movieId) {
        this(requestId, movieId, null);
    }
}
