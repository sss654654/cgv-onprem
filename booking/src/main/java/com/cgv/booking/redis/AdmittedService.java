package com.cgv.booking.redis;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

// admitted = 방송 입장 인증(Set). Kafka admissions로 채워지고, 게이트(SISMEMBER)로 검사(§3-1-3).
// 영화(movie) 단위 — 사람은 방송에 입장하지 관에 입장하는 게 아님(§3-1 단위 결정).
@Service
public class AdmittedService {
    private final StringRedisTemplate redis;
    public AdmittedService(StringRedisTemplate redis) { this.redis = redis; }

    private String key(String movieId) { return "admitted:" + movieId; }

    // Kafka admissions 소비 시 — 입장객 추가.
    public void add(String movieId, String requestId) {
        redis.opsForSet().add(key(movieId), requestId);
    }

    // 게이트(§3-1-3 ①): 이 사람이 입장객이냐. 모든 booking 동작 전 검사.
    public boolean isAdmitted(String movieId, String requestId) {
        Boolean ok = redis.opsForSet().isMember(key(movieId), requestId);
        return Boolean.TRUE.equals(ok);
    }

    // 예매 완료 시 인증 소진(§3-1-7): 재입장 전엔 다시 예매 불가.
    public void remove(String movieId, String requestId) {
        redis.opsForSet().remove(key(movieId), requestId);
    }
}
