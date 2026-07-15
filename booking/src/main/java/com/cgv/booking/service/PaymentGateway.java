package com.cgv.booking.service;

import org.springframework.stereotype.Service;

// PG(결제대행사) mock — §3-1-5. 실제 결제사 안 붙이고 "승인됨" 흉내.
// "여기가 동기 경계"라는 구조만 보임. 보상(환불) 호출도 mock으로 받음.
@Service
public class PaymentGateway {

    // 결제 승인 요청(동기). 데모는 항상 승인. 결제ID 반환(보상 시 사용).
    public String approve(String idempotencyKey, int amount) {
        // 실제라면 외부 HTTP 호출(동기). 멱등키로 같은 결제 두 번 방지.
        return "PAY-" + idempotencyKey;
    }

    // 보상(환불) — DB 커밋 실패 등으로 예매 못 만들면 돈 되돌림(§3-1-6 보상).
    public void refund(String paymentId) {
        // 실제라면 PG 취소 API 호출(멱등). mock은 no-op 로그 수준.
    }
}
