package redis

import (
	"context"
	"log/slog"

	goredis "github.com/redis/go-redis/v9"
)

// promoteScript = 폴링 재설계 기준(백엔드서비스-올인원.md §1-3).
// 대기열 앞에서 빈자리만큼 꺼내 active로 옮긴다(원자).
//   KEYS[1]=waiting, KEYS[2]=active, KEYS[3]=promoted_count, KEYS[4]=waiting_lastseen
//   ARGV[1]=maxSessions(정원), ARGV[2]=batch(한 번에 승격 상한), ARGV[3]=now(ms)
//   반환 = 승격된 requestId 목록
// vacant(빈자리) 계산을 Lua 안에서(max − ZCARD(active)) → 초과승격 차단.
// 승격 시 waiting·waiting_lastseen 둘 다 ZREM(§1-3 정합 규칙).
// INCRBY promoted_count(rate·ETA용, §1-3) — SSE의 processed(자가계산용)와 다른 값.
var promoteScript = goredis.NewScript(`
local waiting = KEYS[1]
local active = KEYS[2]
local promotedCount = KEYS[3]
local lastseen = KEYS[4]
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
end
if #users > 0 then
  redis.call('INCRBY', promotedCount, #users)   -- rate·ETA 계산용 누적
end
return users
`)

// Promote = 정원 빈자리만큼 대기열 앞에서 승격. 승격된 requestId 목록 반환.
// vacant는 Lua 안에서 계산(maxSessions − active) → 정원 초과 불가.
func (c *Client) Promote(ctx context.Context, movieID string, maxSessions, batch, now int64) ([]string, error) {
	keys := []string{WaitingKey(movieID), ActiveKey(movieID), PromotedCountKey(movieID), WaitingLastseenKey(movieID)}
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

// requeueFrontScript = Kafka 발행 실패 승격자의 보상 롤백(설계서 1부 §3-A "(b) 정직한 실패").
// active에서 빼고 waiting "맨 앞"으로 되돌린다 — score 1,2,3…은 기존 score(ms 타임스탬프)보다
// 항상 작아 절대 선두. 다음 성공 틱이 이들부터 재승격하므로 차례를 태우지 않는다.
// promoted_count도 되돌린다(DECRBY) — 롤백된 승격이 rate를 부풀리지 않게.
//   KEYS[1]=active, KEYS[2]=waiting, KEYS[3]=waiting_lastseen, KEYS[4]=promoted_count
//   ARGV[1]=now(ms), ARGV[2..]=requestIds(배치 순서 유지)
var requeueFrontScript = goredis.NewScript(`
local n = #ARGV - 1
for i = 2, #ARGV do
  local m = ARGV[i]
  redis.call('ZREM', KEYS[1], m)
  redis.call('ZADD', KEYS[2], i - 1, m)
  redis.call('ZADD', KEYS[3], tonumber(ARGV[1]), m)
end
if n > 0 then
  redis.call('DECRBY', KEYS[4], n)
end
return n
`)

// RequeueFront = 발행 못 한 승격자들을 waiting 선두로 원자 복귀시킨다(승격 롤백).
func (c *Client) RequeueFront(ctx context.Context, movieID string, requestIDs []string, now int64) error {
	if len(requestIDs) == 0 {
		return nil
	}
	keys := []string{ActiveKey(movieID), WaitingKey(movieID), WaitingLastseenKey(movieID), PromotedCountKey(movieID)}
	argv := make([]interface{}, 0, len(requestIDs)+1)
	argv = append(argv, now)
	for _, id := range requestIDs {
		argv = append(argv, id)
	}
	if err := requeueFrontScript.Run(ctx, c.rdb, keys, argv...).Err(); err != nil {
		return err
	}
	// 대기자가 다시 생겼으니 추적 Set 복구(직전 promote 후처리에서 비었다고 SRem됐을 수 있음).
	if err := c.rdb.SAdd(ctx, WaitingMoviesKey, movieID).Err(); err != nil {
		slog.WarnContext(ctx, "requeue 후처리: waiting_movies 추적 등록 실패", "movie", movieID, "err", err)
	}
	return nil
}
