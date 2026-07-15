package com.cgv.booking.kafka;

// queue ↔ booking 공용 메시지 형식: {requestId, movieId} (§2-4, §3-1-3).
// 토픽 admissions(queue→booking)·bookings-completed(booking→queue) 둘 다 이 모양.
public record QueueEvent(String requestId, String movieId) {}
