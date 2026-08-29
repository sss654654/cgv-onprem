package com.cgv.booking.redis;

import com.cgv.booking.config.CgvProps;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

// admitted = 방송 입장 인증. Kafka admissions로 채워지고, 게이트로 검사.
// 영화(movie) 단위 — 사람은 방송에 입장하지 관에 입장하는 게 아님.
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

    // 배치 소비용 인증 항목.
    public record Admission(String movieId, String requestId) {}

    // 여러 명의 인증을 파이프라인 한 왕복으로 발급한다.
    // 한 명씩 add()를 부르면 명수만큼 왕복한다 — 예매 오픈 순간 수백 명의 admissions가 몰릴 때
    // 그 왕복 수가 소비 속도의 상한이 됐다(2026-08-29 사용자 10,000 부하: 소비 초당 약 28건,
    // 인증 지연 p99 9.94초). 파이프라인은 명령을 몰아 보내고 응답을 몰아 받아 왕복이 1회다.
    // 각 SET은 여전히 개별 명령이라 멱등성은 add()와 같다 — 중복 수신은 TTL 재설정.
    // 연결 실패는 예외로 전파된다 → 리스너가 배치를 재시도한다(멱등이라 재처리 무해).
    public void addAll(List<Admission> admissions) {
        byte[] one = "1".getBytes(StandardCharsets.UTF_8);
        Expiration expiration = Expiration.from(ttl);
        redis.executePipelined((RedisCallback<Object>) conn -> {
            for (Admission a : admissions) {
                conn.stringCommands().set(
                        key(a.movieId(), a.requestId()).getBytes(StandardCharsets.UTF_8),
                        one, expiration, RedisStringCommands.SetOption.UPSERT);
            }
            return null;
        });
    }

    // 게이트: 이 사람이 입장객이냐. 모든 booking 동작 전 검사.
    // TTL 만료 = 자동 미입장 → queue 세션 타임아웃과 booking 게이트가 다시 붙는다.
    public boolean isAdmitted(String movieId, String requestId) {
        return Boolean.TRUE.equals(redis.hasKey(key(movieId, requestId)));
    }

    // 인증 소진: 예매 완료 시(재입장 전엔 다시 예매 불가) / queue 해소 이벤트 수신 시.
    public void remove(String movieId, String requestId) {
        redis.delete(key(movieId, requestId));
    }
}
