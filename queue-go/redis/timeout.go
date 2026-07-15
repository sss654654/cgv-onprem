package redis

import (
	"context"

	goredis "github.com/redis/go-redis/v9"
)

// expireScript = active에서 score(=입장시각ms) ≤ cutoff 인 멤버를 찾아 제거(원자, §1-4a).
// ZSet 멤버엔 개별 TTL을 못 거니, score를 직접 스캔해 수동 만료한다.
//   KEYS[1]=active, ARGV[1]=cutoff(ms)
//   반환 = 만료된 requestId 목록
var expireScript = goredis.NewScript(`
-- LIMIT 1000: unpack 인자 한계 방지. 넘치면 다음 틱이 이어서 처리(루프가 주기적).
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'LIMIT', 0, 1000)
if #expired > 0 then
  redis.call('ZREM', KEYS[1], unpack(expired))
end
return expired
`)

// ExpireActive = 입장한 지 오래된(score ≤ cutoff) active 세션을 제거하고 목록 반환.
func (c *Client) ExpireActive(ctx context.Context, movieID string, cutoff int64) ([]string, error) {
	return runExpire(ctx, c.rdb, expireScript, cutoff, ActiveKey(movieID))
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
	arr, _ := raw.([]interface{})
	out := make([]string, 0, len(arr))
	for _, v := range arr {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out, nil
}
