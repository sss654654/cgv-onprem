package redis

import (
	"context"
	"fmt"
	"log/slog"

	goredis "github.com/redis/go-redis/v9"
)

// enterScript = 폴링 재설계 기준(백엔드서비스-올인원.md §1-1).
// "정원 확인 → 입장 or 대기"를 원자 처리.
//   KEYS[1]=active, KEYS[2]=waiting, KEYS[3]=waiting_lastseen
//   ARGV[1]=maxSessions, ARGV[2]=member(requestId), ARGV[3]=now(ms)
// 반환(상태 1=active경로 / 2=waiting경로):
//   {1,'ALREADY_ACTIVE', activeCount}         이미 입장 → 자리 유지(§1-1 새로고침 정책)
//   {1,'ADMITTED', activeCount+1}
//   {2,'WAITING', rank+1, totalWaiting}
// 새로고침 정책(§1-1): active 재진입=유지 / waiting 재진입=꼬리로 밀기(ZREM+ZADD).
// waiting 경로는 waiting·waiting_lastseen 둘 다 ZADD(생존 추적 시작, §1-4b).
// baseline·processed 제거 — 폴링은 순번을 서버가 ZRANK로 직접 주므로 자가계산 불필요.
var enterScript = goredis.NewScript(`
local active = KEYS[1]
local waiting = KEYS[2]
local lastseen = KEYS[3]
local maxSessions = tonumber(ARGV[1])
local member = ARGV[2]
local now = tonumber(ARGV[3])

if redis.call('ZSCORE', active, member) then
  return {1, 'ALREADY_ACTIVE', redis.call('ZCARD', active)}
end
if redis.call('ZSCORE', waiting, member) then
  -- 새로고침 정책: waiting 재진입 = 꼬리로 밀기(같은 member 제거·재삽입이라 고아 없음)
  redis.call('ZREM', waiting, member)
  redis.call('ZADD', waiting, now, member)
  redis.call('ZADD', lastseen, now, member)
  local rank = redis.call('ZRANK', waiting, member)
  return {2, 'WAITING', rank + 1, redis.call('ZCARD', waiting)}
end
local activeCount = redis.call('ZCARD', active)
if activeCount < maxSessions then
  redis.call('ZADD', active, now, member)
  return {1, 'ADMITTED', activeCount + 1}
else
  redis.call('ZADD', waiting, now, member)
  redis.call('ZADD', lastseen, now, member)
  local rank = redis.call('ZRANK', waiting, member)
  return {2, 'WAITING', rank + 1, redis.call('ZCARD', waiting)}
end
`)

// EnterResult = enter Lua 반환을 Go 구조체로.
type EnterResult struct {
	Status int    // 1=active경로(ADMITTED/ALREADY_ACTIVE), 2=waiting경로
	Code   string // ADMITTED / WAITING / ALREADY_ACTIVE
	Rank   int64  // waiting 경로면 1-based 순번, active면 0
	Count  int64  // active경로=현재 active 인원 / waiting경로=전체 대기 인원
}

// Enter = enter Lua를 실행하고, 후처리로 영화를 추적 Set에 등록한다.
func (c *Client) Enter(ctx context.Context, movieID, requestID string, maxSessions, now int64) (EnterResult, error) {
	keys := []string{ActiveKey(movieID), WaitingKey(movieID), WaitingLastseenKey(movieID)}
	raw, err := enterScript.Run(ctx, c.rdb, keys, maxSessions, requestID, now).Result()
	if err != nil {
		return EnterResult{}, err
	}

	arr, ok := raw.([]interface{})
	if !ok || len(arr) < 3 {
		return EnterResult{}, fmt.Errorf("unexpected enter result: %v", raw)
	}
	status, _ := arr[0].(int64)
	code, _ := arr[1].(string)
	res := EnterResult{Status: int(status), Code: code}
	if status == 2 { // waiting 경로: rank, total
		res.Rank, _ = arr[2].(int64)
		if len(arr) >= 4 {
			res.Count, _ = arr[3].(int64)
		}
	} else { // active 경로: arr[2]=active 인원
		res.Count, _ = arr[2].(int64)
	}

	// 후처리: 트래픽 있는 영화 추적(승격·타임아웃 루프가 이 Set으로 대상 영화를 찾음).
	// 실패 시 로그 필수 — 조용히 빠지면 그 영화를 루프가 안 돌아 승격이 멈춘다.
	if err := c.rdb.SAdd(ctx, ActiveMoviesKey, movieID).Err(); err != nil {
		slog.WarnContext(ctx, "enter 후처리: active_movies 추적 등록 실패", "movie", movieID, "err", err)
	}
	if status == 2 {
		if err := c.rdb.SAdd(ctx, WaitingMoviesKey, movieID).Err(); err != nil {
			slog.WarnContext(ctx, "enter 후처리: waiting_movies 추적 등록 실패", "movie", movieID, "err", err)
		}
	}

	return res, nil
}
