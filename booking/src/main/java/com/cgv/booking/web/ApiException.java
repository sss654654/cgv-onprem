package com.cgv.booking.web;

import org.springframework.http.HttpStatus;

// 도메인 오류 → HTTP 상태 매핑용. 403(미입장)·409(좌석 선점/만료) 등.
public class ApiException extends RuntimeException {
    private final HttpStatus status;
    private final String code;
    public ApiException(HttpStatus status, String code, String message) {
        super(message);
        this.status = status;
        this.code = code;
    }
    public HttpStatus getStatus() { return status; }
    public String getCode() { return code; }

    public static ApiException forbidden(String msg) { return new ApiException(HttpStatus.FORBIDDEN, "NOT_ADMITTED", msg); }
    public static ApiException conflict(String msg) { return new ApiException(HttpStatus.CONFLICT, "SEAT_CONFLICT", msg); }
}
