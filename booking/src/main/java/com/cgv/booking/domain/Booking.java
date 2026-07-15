package com.cgv.booking.domain;

import jakarta.persistence.*;
import org.springframework.data.domain.Persistable;
import java.time.LocalDateTime;

// 예매 확정(bookings) — 영구 기록. 좌석은 booking_seats에 따로(UNIQUE 걸려고, §3-1-8).
// idempotency_key UNIQUE = 결제 더블클릭 차단(§3-1-5).
// id를 앱이 미리 채우므로(BK-xxxx) Persistable로 isNew=true를 명시한다 — 안 하면 Spring Data가
// "id != null = 기존 엔티티"로 보고 save()가 persist 대신 merge(SELECT 선행)를 호출한다.
// 확정 트랜잭션이 DB 커넥션 풀 병목(X 사이징)이라, 확정마다 붙던 불필요한 SELECT 1회를 없앤다.
@Entity
@Table(name = "bookings",
        uniqueConstraints = @UniqueConstraint(columnNames = {"idempotency_key"}))
public class Booking implements Persistable<String> {
    @Id
    private String id;                 // BK-xxxx

    @Column(name = "screening_id")
    private String screeningId;

    @Column(name = "user_id")
    private String userId;             // requestId

    private int price;                 // 좌석수 × 6,000

    @Column(name = "idempotency_key")
    private String idempotencyKey;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Transient
    private boolean isNew = true;      // 새 엔티티 판정용. @Transient라 컬럼 아님. 영속/로드 후 false.

    protected Booking() {}
    public Booking(String id, String screeningId, String userId, int price, String idempotencyKey) {
        this.id = id; this.screeningId = screeningId; this.userId = userId;
        this.price = price; this.idempotencyKey = idempotencyKey; this.createdAt = LocalDateTime.now();
    }

    @Override
    public String getId() { return id; }

    @Override
    public boolean isNew() { return isNew; }

    // INSERT 성공 또는 DB 로드 직후 = 더 이상 새 엔티티 아님.
    @PostPersist
    @PostLoad
    void markNotNew() { this.isNew = false; }

    public String getScreeningId() { return screeningId; }
    public String getUserId() { return userId; }
    public int getPrice() { return price; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public LocalDateTime getCreatedAt() { return createdAt; }
}
