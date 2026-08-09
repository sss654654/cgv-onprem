package redis

import (
	"context"
	"log/slog"

	goredis "github.com/redis/go-redis/v9"
)

// promoteScript = 폴링 재설계 기준.
// 대기열 앞에서 빈자리만큼 꺼내 active로 옮긴다(원자).
//   KEYS[1]=waiting, KEYS[2]=active, KEYS[3]=promoted_count, KEYS[4]=waiting_lastseen,
//   KEYS[5]=pending_events
//   ARGV[1]=maxSessions(정원), ARGV[2]=batch(한 번에 승격 상한), ARGV[3]=now(ms)
//   반환 = 승격된 requestId 목록
// vacant(빈자리) 계산을 Lua 안에서(max − ZCARD(active)) → 초과승격 차단.
// 승격 시 waiting·waiting_lastseen 둘 다 ZREM.
// INCRBY promoted_count(rate·ETA용) — SSE의 processed(자가계산용)와 다른 값.
// 승격자는 발행 대기 저널(pending_events)에도 같은 원자 실행 안에서 남긴다 — 이 Lua가 끝난 뒤
// 발행 전에 파드가 죽어도 승격자 명단이 Redis에 남아 다른 파드가 이어서 발행한다.
var promoteScript = goredis.NewScript(`
local waiting = KEYS[1]
local active = KEYS[2]
local promotedCount = KEYS[3]
local lastseen = KEYS[4]
local pending = KEYS[5]
local maxSessions = tonumber(ARGV[1])
local batch = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local activeCount = redis.call('ZCARD', active)
local vacant = maxSessions - activeCount
if vacant <= 0 then return {} end
local n = vacant
if batch < n then n = batch end
-- 가드: n<=0이면 즉시 종료. ZRANGE(0, -1)은 "전체"라 batch=0 설정 실수 시 전원 승격됨.
if n <= 0 then return {} end

local users = redis.call('ZRANGE', waiting, 0, n - 1)   -- 앞에서 n명(score순=선착순)
for i = 1, #users do
  redis.call('ZREM', waiting, users[i])
  redis.call('ZREM', lastseen, users[i])
  redis.call('ZADD', active, now, users[i])
  redis.call('ZADD', pending, now, 'A|PROMOTE|' .. users[i])
end
if #users > 0 then
  redis.call('INCRBY', promotedCount, #users)   -- rate·ETA 계산용 누적
end
return users
`)

// Promote = 정원 빈자리만큼 대기열 앞에서 승격. 승격된 requestId 목록 반환.
// vacant는 Lua 안에서 계산(maxSessions − active) → 정원 초과 불가.
func (c *Client) Promote(ctx context.Context, movieID string, maxSessions, batch, now int64) ([]string, error) {
	keys := []string{WaitingKey(movieID), ActiveKey(movieID), PromotedCountKey(movieID), WaitingLastseenKey(movieID), PendingKey(movieID)}
	raw, err := promoteScript.Run(ctx, c.rdb, keys, maxSessions, batch, now).Result()
	if err != nil {
		return nil, err
	}

	// 대기열이 비면 추적 Set에서 제거(승격 루프가 빈 영화를 안 돌게).
	if n, _ := c.rdb.ZCard(ctx, WaitingKey(movieID)).Result(); n == 0 {
		if err := c.rdb.SRem(ctx, WaitingMoviesKey, movieID).Err(); err != nil {
			slog.WarnContext(ctx, "promote 후처리: waiting_movies 추적 해제 실패", "movie", movieID, "err", err)
		}
	}

	// Lua는 문자열 배열을 []interface{}로 돌려줌 → []string으로.
	arr, _ := raw.([]interface{})
	admitted := make([]string, 0, len(arr))
	for _, v := range arr {
		if s, ok := v.(string); ok {
			admitted = append(admitted, s)
		}
	}
	return admitted, nil
}

// 발행 실패 시의 보상 롤백(승격자를 waiting 선두로 되돌리기)은 제거했다.
// 롤백은 "발행 실패 = 브로커에 안 들어감"을 전제하는데, ctx 취소·타임아웃은 브로커 append
// 이후에도 에러로 돌아온다 → booking엔 admitted가 들어갔는데 queue가 active를 되돌려
// 정원 밖 입장자가 생긴다. 대신 발행 대기 저널(pending_events)에 남겨 두고 sweep이 재발행한다:
// admissions 소비는 SADD라 멱등이므로 중복 발행은 무해하고, 유실만 막으면 된다.
