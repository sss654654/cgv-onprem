package com.cgv.booking.redis;

import com.cgv.booking.config.CgvProps;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

// 좌석 임시점유 — string 키 seat:{screeningId}:{seatNo}, SET NX EX(TTL).
// 좌석마다 키 1개(독립 NX·TTL·DEL). 다중좌석은 Lua로 all-or-nothing(원자).
//
// 인덱스: 회차별 Set seatlocks:{screeningId}에 점유 중인 좌석번호를 같은 Lua 안에서 함께 기록한다.
//   이게 없으면 "이 회차에 몇 석 잠겼나"를 SCAN으로 알아내야 하는데, SCAN의 MATCH는 서버측 필터일 뿐
//   커서는 DB 전체 키를 훑는다 → 회차 1개 조회에 (전체 키수 / COUNT)회 왕복. 회차 20개면 요청당 수백 왕복이고,
//   이 Redis는 queue가 대기열 폴링을 처리하는 바로 그 인스턴스다.
//   인덱스로 바꾸면 회차당 1왕복(멤버 ≤ 좌석수)이 된다.
// 인덱스의 stale 문제: 좌석 키는 TTL로 사라지지만 Set 멤버는 안 사라진다 → 조회 Lua가 멤버마다
//   EXISTS로 확인하고 없는 멤버는 그 자리에서 SREM(자가 정리). 그래서 인덱스는 항상 실제 점유의 상위집합이고,
//   반환값은 실제 점유와 정확히 일치한다.
// 전제: 단일 마스터 Redis(로컬 standalone / dev sentinel). Redis Cluster면 Lua가 KEYS에 없는 키를
//   건드리므로 키슬롯 규칙 위반이 된다 — 클러스터 전환 시 해시태그로 회차별 슬롯 고정이 필요하다.
@Service
public class SeatLockService {
    private final StringRedisTemplate redis;
    private final int ttl;

    // 다중 점유: 하나라도 있으면 0(아무것도 안 잠금), 다 비면 전부 NX EX → 1.
    // 성공 시 인덱스 Set에도 좌석번호를 SADD(같은 원자 단위 — 잠갔는데 인덱스에 없는 상태를 만들지 않는다).
    // KEYS = 좌석 키들, ARGV[1]=owner, ARGV[2]=ttl, ARGV[3]=인덱스 Set 키, ARGV[4..]=좌석번호들
    private static final DefaultRedisScript<Long> LOCK = new DefaultRedisScript<>(
            "for i=1,#KEYS do if redis.call('EXISTS', KEYS[i]) == 1 then return 0 end end " +
            "for i=1,#KEYS do redis.call('SET', KEYS[i], ARGV[1], 'NX', 'EX', ARGV[2]) end " +
            "for i=4,#ARGV do redis.call('SADD', ARGV[3], ARGV[i]) end " +
            "return 1", Long.class);

    // 내 락만 해제(뒤로가기/취소): 값이 나(userId)인 키만 DEL. 남의 락은 안 건드림.
    // 지운 좌석만 인덱스에서도 뺀다(남의 락은 인덱스에 남아야 한다).
    // KEYS = 좌석 키들, ARGV[1]=owner, ARGV[2]=인덱스 Set 키, ARGV[3..]=좌석번호들(KEYS와 같은 순서)
    private static final DefaultRedisScript<Long> RELEASE = new DefaultRedisScript<>(
            "for i=1,#KEYS do " +
            "  if redis.call('GET', KEYS[i]) == ARGV[1] then " +
            "    redis.call('DEL', KEYS[i]) redis.call('SREM', ARGV[2], ARGV[i+2]) " +
            "  end " +
            "end return 1", Long.class);

    // 결제 진입 시 락 확인+연장: 전부 내 락일 때만 TTL 연장(원자). 하나라도 아니면 0(연장 안 함).
    // EXPIRE만 쓰면 남의 락도 연장하는 사고 → 반드시 "값이 내 것"을 먼저 검사. (정합 최종방어는 여전히 UNIQUE)
    private static final DefaultRedisScript<Long> RENEW = new DefaultRedisScript<>(
            "for i=1,#KEYS do if redis.call('GET', KEYS[i]) ~= ARGV[1] then return 0 end end " +
            "for i=1,#KEYS do redis.call('EXPIRE', KEYS[i], ARGV[2]) end " +
            "return 1", Long.class);

    // 회차의 실제 점유 좌석번호 조회 + 인덱스 자가 정리(TTL로 사라진 멤버 SREM).
    // KEYS[1]=인덱스 Set 키, ARGV[1]=좌석 키 접두사.
    private static final DefaultRedisScript<List> LIVE_LOCKS = new DefaultRedisScript<>(
            "local members = redis.call('SMEMBERS', KEYS[1]) " +
            "local live = {} " +
            "local stale = {} " +
            "for i=1,#members do " +
            "  if redis.call('EXISTS', ARGV[1] .. members[i]) == 1 then live[#live+1] = members[i] " +
            "  else stale[#stale+1] = members[i] end " +
            "end " +
            "for i=1,#stale do redis.call('SREM', KEYS[1], stale[i]) end " +
            "return live", List.class);

    public SeatLockService(StringRedisTemplate redis, CgvProps props) {
        this.redis = redis;
        this.ttl = props.getSeatLockTtlSeconds();
    }

    public static String key(String screeningId, String seatNo) {
        return "seat:" + screeningId + ":" + seatNo;
    }

    private static String prefix(String screeningId) {
        return "seat:" + screeningId + ":";
    }

    private static String indexKey(String screeningId) {
        return "seatlocks:" + screeningId;
    }

    private List<String> keys(String screeningId, List<String> seatNos) {
        List<String> ks = new ArrayList<>(seatNos.size());
        for (String s : seatNos) ks.add(key(screeningId, s));
        return ks;
    }

    // 좌석 점유 시도. true=전부 점유 성공 / false=하나라도 선점됨(409).
    public boolean lockAll(String screeningId, List<String> seatNos, String userId) {
        List<String> argv = new ArrayList<>(seatNos.size() + 3);
        argv.add(userId);
        argv.add(String.valueOf(ttl));
        argv.add(indexKey(screeningId));
        argv.addAll(seatNos);
        Long r = redis.execute(LOCK, keys(screeningId, seatNos), argv.toArray());
        return r != null && r == 1L;
    }

    // 내 점유 해제(뒤로가기 등).
    public void releaseMine(String screeningId, List<String> seatNos, String userId) {
        List<String> argv = new ArrayList<>(seatNos.size() + 2);
        argv.add(userId);
        argv.add(indexKey(screeningId));
        argv.addAll(seatNos);
        redis.execute(RELEASE, keys(screeningId, seatNos), argv.toArray());
    }

    // 결제 진입 시: 내 락이 전부 살아있으면 TTL 연장(true) / 하나라도 아니면 연장 안 함(false, =409).
    // PG 승인 도는 사이 락 만료 → 남이 선점 → UNIQUE 위반 환불의 UX를 줄인다.
    public boolean renewMine(String screeningId, List<String> seatNos, String userId) {
        Long r = redis.execute(RENEW, keys(screeningId, seatNos), userId, String.valueOf(ttl));
        return r != null && r == 1L;
    }

    // 좌석도: 현재 임시점유 중인 좌석번호 집합. 회차당 왕복 1회.
    @SuppressWarnings("unchecked")
    public Set<String> lockedSeatNos(String screeningId) {
        List<Object> live = redis.execute(LIVE_LOCKS,
                Collections.singletonList(indexKey(screeningId)), prefix(screeningId));
        if (live == null) return Set.of();
        Set<String> out = new HashSet<>(live.size());
        for (Object o : live) out.add(String.valueOf(o));
        return out;
    }

    // 회차 선택: 그 관 임시점유 수.
    public long countLocked(String screeningId) {
        return lockedSeatNos(screeningId).size();
    }
}
