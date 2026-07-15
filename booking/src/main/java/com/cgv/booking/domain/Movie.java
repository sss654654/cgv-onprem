package com.cgv.booking.domain;

import jakarta.persistence.*;
import java.time.LocalDateTime;

// 영화(방송) — 생중계라 1개, 시각 단일(§3-1-8). queue/admitted는 이 movieId 단위.
@Entity
@Table(name = "movies")
public class Movie {
    @Id
    private String id;                 // 예: kbo-allstar-2025 (queue movieId와 동일)
    private String title;
    private LocalDateTime broadcastAt; // 18:00 단일

    protected Movie() {}
    public Movie(String id, String title, LocalDateTime broadcastAt) {
        this.id = id; this.title = title; this.broadcastAt = broadcastAt;
    }
    public String getId() { return id; }
    public String getTitle() { return title; }
    public LocalDateTime getBroadcastAt() { return broadcastAt; }
}
