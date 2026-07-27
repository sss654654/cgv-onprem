package com.cgv.booking.redis;

import com.cgv.booking.config.CgvProps;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

// admitted = 방송 입장 인증. Kafka admissions로 채워지고, 게이트로 검사(§3-1-3).
// 영화(movie) 단위 — 사람은 방송에 입장하지 관에 입장하는 게 아님(§3-1 단위 결정).
//
// 표현: Set(SADD/SISMEMBER)이 아니라 "멤버당 키 1개 + TTL"이다.
//   이유 — 입장 인증은 queue의 세션 타임아웃과 함께 사라져야 하는데, Redis Set은 멤버별 TTL이 없다.
//   Set을 쓰면 SREM이 예매 확정 성공 한 곳뿐이라 세션 타임아웃으로 자리를 잃은 사람이 계속 게이트를
//   통과하고(정원 무력화), Set이 무한 증가한다. 멤버당 키로 바꾸면 만료가 Redis 자체 기능이 된다.
//   비용: 키 개수가 (입장 정원)만큼 늘어난다 — 정원 상한이 곧 키 상한이라 유계.
@Service
public class AdmittedService {
    private final StringRedisTemplate redis;
    private final Duration ttl;

    public AdmittedService(StringRedisTemplate redis, CgvProps props) {
        this.redis = redis;
        this.ttl = Duration.ofSeconds(props.getAdmittedTtlSeconds());
    }

    // 키 = admitted:{movieId}:{requestId}. movieId/requestId는 ASCII 식별자.
    private String key(String movieId, String requestId) {
        return "admitted:" + movieId + ":" + requestId;
    }

    // Kafka admissions 소비 시 — 입장 인증 발급(TTL 동반). 중복 수신은 TTL 재설정만 하고 무해(멱등).
    public void add(String movieId, String requestId) {
        redis.opsForValue().set(key(movieId, requestId), "1", ttl);
    }

    // 게이트(§3-1-3 ①): 이 사람이 입장객이냐. 모든 booking 동작 전 검사.
    // TTL 만료 = 자동 미입장 → queue 세션 타임아웃과 booking 게이트가 다시 붙는다.
    public boolean isAdmitted(String movieId, String requestId) {
        return Boolean.TRUE.equals(redis.hasKey(key(movieId, requestId)));
    }

    // 인증 소진: 예매 완료 시(재입장 전엔 다시 예매 불가) / queue 해소 이벤트 수신 시.
    public void remove(String movieId, String requestId) {
        redis.delete(key(movieId, requestId));
    }
}
