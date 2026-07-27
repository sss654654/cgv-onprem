package redis

import (
	"context"

	goredis "github.com/redis/go-redis/v9"
)

// expireScript = active에서 score(=입장시각ms) ≤ cutoff 인 멤버를 찾아 제거(원자, §1-4a).
// ZSet 멤버엔 개별 TTL을 못 거니, score를 직접 스캔해 수동 만료한다.
// 제거와 동시에 회수 이벤트를 발행 대기 저널에 남긴다 — active에서 빠진 사람의 admitted가
// booking에 그대로 남으면 정원 밖 사용자가 계속 예매 API를 통과한다.
// 아직 발행 못 한 입장 통보는 같이 지운다(sweep이 뒤늦게 발행해 인증이 되살아나지 않게).
//   KEYS[1]=active, KEYS[2]=pending_events, ARGV[1]=cutoff(ms), ARGV[2]=now(ms)
//   반환 = 만료된 requestId 목록
var expireScript = goredis.NewScript(`
-- LIMIT 1000: unpack 인자 한계 방지. 넘치면 다음 틱이 이어서 처리(루프가 주기적).
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'LIMIT', 0, 1000)
local now = tonumber(ARGV[2])
if #expired > 0 then
  redis.call('ZREM', KEYS[1], unpack(expired))
  for i = 1, #expired do
    redis.call('ZREM', KEYS[2], 'A|ENTER|' .. expired[i], 'A|PROMOTE|' .. expired[i])
    redis.call('ZADD', KEYS[2], now, 'R|SESSION_TIMEOUT|' .. expired[i])
  end
end
return expired
`)

// ExpireActive = 입장한 지 오래된(score ≤ cutoff) active 세션을 제거하고 목록 반환.
// 회수 이벤트는 저널에만 남기고, 실제 Kafka 발행은 호출자(타임아웃 루프)가 한다.
func (c *Client) ExpireActive(ctx context.Context, movieID string, cutoff, now int64) ([]string, error) {
	raw, err := expireScript.Run(ctx, c.rdb, []string{ActiveKey(movieID), PendingKey(movieID)}, cutoff, now).Result()
	if err != nil {
		return nil, err
	}
	return toStrings(raw), nil
}

// waitingExpireScript = 폴링 타임아웃(§1-4b). 마지막 폴링(lastseen) ≤ cutoff 인 대기자를
// waiting·waiting_lastseen 둘 다에서 제거(원자, §1-3 정합 규칙).
//   KEYS[1]=waiting, KEYS[2]=waiting_lastseen, ARGV[1]=cutoff(ms)
//   반환 = evict된 requestId 목록
var waitingExpireScript = goredis.NewScript(`
-- LIMIT 1000: unpack 인자 한계 방지. 넘치면 다음 틱이 이어서 처리.
local stale = redis.call('ZRANGEBYSCORE', KEYS[2], 0, ARGV[1], 'LIMIT', 0, 1000)
if #stale > 0 then
  redis.call('ZREM', KEYS[2], unpack(stale))
  redis.call('ZREM', KEYS[1], unpack(stale))
end
return stale
`)

// ExpireWaiting = 폴링이 끊긴(lastseen ≤ cutoff) 대기자를 evict하고 목록 반환.
func (c *Client) ExpireWaiting(ctx context.Context, movieID string, cutoff int64) ([]string, error) {
	return runExpire(ctx, c.rdb, waitingExpireScript, cutoff, WaitingKey(movieID), WaitingLastseenKey(movieID))
}

// runExpire = Lua 실행 후 반환 배열을 []string으로.
func runExpire(ctx context.Context, rdb *goredis.Client, script *goredis.Script, cutoff int64, keys ...string) ([]string, error) {
	raw, err := script.Run(ctx, rdb, keys, cutoff).Result()
	if err != nil {
		return nil, err
	}
	return toStrings(raw), nil
}

// toStrings = Lua가 돌려준 배열([]interface{})에서 문자열만 추린다.
func toStrings(raw interface{}) []string {
	arr, _ := raw.([]interface{})
	out := make([]string, 0, len(arr))
	for _, v := range arr {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}
