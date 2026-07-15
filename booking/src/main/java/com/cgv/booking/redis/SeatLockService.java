package com.cgv.booking.redis;

import com.cgv.booking.config.CgvProps;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

// 좌석 임시점유(§3-1-3) — string 키 seat:{screeningId}:{seatNo}, SET NX EX(TTL 5분).
// 좌석마다 키 1개(독립 NX·TTL·DEL). 다중좌석은 Lua로 all-or-nothing(원자).
@Service
public class SeatLockService {
    private final StringRedisTemplate redis;
    private final int ttl;

    // 다중 점유: 하나라도 있으면 0(아무것도 안 잠금), 다 비면 전부 NX EX → 1.
    private static final DefaultRedisScript<Long> LOCK = new DefaultRedisScript<>(
            "for i=1,#KEYS do if redis.call('EXISTS', KEYS[i]) == 1 then return 0 end end " +
            "for i=1,#KEYS do redis.call('SET', KEYS[i], ARGV[1], 'NX', 'EX', ARGV[2]) end " +
            "return 1", Long.class);

    // 내 락만 해제(뒤로가기/취소): 값이 나(userId)인 키만 DEL. 남의 락은 안 건드림.
    private static final DefaultRedisScript<Long> RELEASE = new DefaultRedisScript<>(
            "for i=1,#KEYS do if redis.call('GET', KEYS[i]) == ARGV[1] then redis.call('DEL', KEYS[i]) end end " +
            "return 1", Long.class);

    // 결제 진입 시 락 확인+연장(§2-D [7]): 전부 내 락일 때만 TTL 연장(원자). 하나라도 아니면 0(연장 안 함).
    // EXPIRE만 쓰면 남의 락도 연장하는 사고 → 반드시 "값이 내 것"을 먼저 검사. (정합 최종방어는 여전히 UNIQUE)
    private static final DefaultRedisScript<Long> RENEW = new DefaultRedisScript<>(
            "for i=1,#KEYS do if redis.call('GET', KEYS[i]) ~= ARGV[1] then return 0 end end " +
            "for i=1,#KEYS do redis.call('EXPIRE', KEYS[i], ARGV[2]) end " +
            "return 1", Long.class);

    public SeatLockService(StringRedisTemplate redis, CgvProps props) {
        this.redis = redis;
        this.ttl = props.getSeatLockTtlSeconds();
    }

    public static String key(String screeningId, String seatNo) {
        return "seat:" + screeningId + ":" + seatNo;
    }

    private List<String> keys(String screeningId, List<String> seatNos) {
        List<String> ks = new ArrayList<>(seatNos.size());
        for (String s : seatNos) ks.add(key(screeningId, s));
        return ks;
    }

    // 좌석 점유 시도. true=전부 점유 성공 / false=하나라도 선점됨(409).
    public boolean lockAll(String screeningId, List<String> seatNos, String userId) {
        Long r = redis.execute(LOCK, keys(screeningId, seatNos), userId, String.valueOf(ttl));
        return r != null && r == 1L;
    }

    // 내 점유 해제(뒤로가기 등).
    public void releaseMine(String screeningId, List<String> seatNos, String userId) {
        redis.execute(RELEASE, keys(screeningId, seatNos), userId);
    }

    // 결제 진입 시: 내 락이 전부 살아있으면 TTL 연장(true) / 하나라도 아니면 연장 안 함(false, =409).
    // PG 승인 도는 사이 락 만료 → 남이 선점 → UNIQUE 위반 환불의 UX를 줄인다(§2-D [7]).
    public boolean renewMine(String screeningId, List<String> seatNos, String userId) {
        Long r = redis.execute(RENEW, keys(screeningId, seatNos), userId, String.valueOf(ttl));
        return r != null && r == 1L;
    }

    // 좌석도(§3-1-2): 현재 임시점유 중인 좌석번호 집합. KEYS(O(N) 블로킹) 대신 SCAN(커서, 비블로킹).
    public Set<String> lockedSeatNos(String screeningId) {
        String prefix = "seat:" + screeningId + ":";
        java.util.HashSet<String> out = new java.util.HashSet<>();
        ScanOptions opts = ScanOptions.scanOptions().match(prefix + "*").count(200).build();
        try (Cursor<String> cur = redis.scan(opts)) {
            while (cur.hasNext()) out.add(cur.next().substring(prefix.length()));
        }
        return out;
    }

    // 회차 선택(§3-1-1): 그 관 임시점유 수. SCAN은 중복 반환 가능 → distinct 집합(lockedSeatNos) 크기로.
    // (n++ 방식은 SCAN 중복 시 과대집계. 정합엔 무관하나 표시값 정확도.)
    public long countLocked(String screeningId) {
        return lockedSeatNos(screeningId).size();
    }
}
