package redis

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

// enterScript = 폴링 재설계 기준.
// "정원 확인 → 입장 or 대기"를 원자 처리.
//   KEYS[1]=active, KEYS[2]=waiting, KEYS[3]=waiting_lastseen, KEYS[4]=pending_events,
//   KEYS[5]=active_movies, KEYS[6]=waiting_movies
//   ARGV[1]=maxSessions, ARGV[2]=member(requestId), ARGV[3]=now(ms), ARGV[4]=movieId
// 반환(상태 1=active경로 / 2=waiting경로):
//   {1,'ALREADY_ACTIVE', activeCount}         이미 입장 → 자리 유지
//   {1,'ADMITTED', activeCount+1}
//   {2,'WAITING', rank+1, totalWaiting}
// 새로고침 정책: active 재진입=유지 / waiting 재진입=꼬리로 밀기(ZREM+ZADD).
// waiting 경로는 waiting·waiting_lastseen 둘 다 ZADD(생존 추적 시작).
// baseline·processed 제거 — 폴링은 순번을 서버가 ZRANK로 직접 주므로 자가계산 불필요.
// ADMITTED면 발행 대기 저널(pending_events)에도 같은 원자 실행 안에서 기록한다 —
// active 등록과 "booking에 알릴 의무"가 갈라지지 않게(발행 전 즉사해도 저널이 남는다).
// 영화 추적 Set(active_movies·waiting_movies) 등록도 같은 원자 실행 안에서 한다 —
// 등록이 Lua 밖 별도 호출이면 그 사이 파드가 죽었을 때 이 영화가 승격·타임아웃·스윕
// 루프 대상에서 빠져, 대기자가 있는데 승격이 멈춘다(다음 enter까지).
var enterScript = goredis.NewScript(`
local active = KEYS[1]
local waiting = KEYS[2]
local lastseen = KEYS[3]
local pending = KEYS[4]
local activeMovies = KEYS[5]
local waitingMovies = KEYS[6]
local maxSessions = tonumber(ARGV[1])
local member = ARGV[2]
local now = tonumber(ARGV[3])
local movie = ARGV[4]

redis.call('SADD', activeMovies, movie)
if redis.call('ZSCORE', active, member) then
  return {1, 'ALREADY_ACTIVE', redis.call('ZCARD', active)}
end
if redis.call('ZSCORE', waiting, member) then
  -- 새로고침 정책: waiting 재진입 = 꼬리로 밀기(같은 member 제거·재삽입이라 고아 없음)
  redis.call('SADD', waitingMovies, movie)
  redis.call('ZREM', waiting, member)
  redis.call('ZADD', waiting, now, member)
  redis.call('ZADD', lastseen, now, member)
  local rank = redis.call('ZRANK', waiting, member)
  return {2, 'WAITING', rank + 1, redis.call('ZCARD', waiting)}
end
local activeCount = redis.call('ZCARD', active)
if activeCount < maxSessions then
  redis.call('ZADD', active, now, member)
  redis.call('ZADD', pending, now, 'A|ENTER|' .. member)
  return {1, 'ADMITTED', activeCount + 1}
else
  redis.call('SADD', waitingMovies, movie)
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

// Enter = enter Lua를 실행한다. 영화 추적 Set 등록까지 Lua 안에서 끝난다.
func (c *Client) Enter(ctx context.Context, movieID, requestID string, maxSessions, now int64) (EnterResult, error) {
	keys := []string{ActiveKey(movieID), WaitingKey(movieID), WaitingLastseenKey(movieID), PendingKey(movieID), ActiveMoviesKey, WaitingMoviesKey}
	raw, err := enterScript.Run(ctx, c.rdb, keys, maxSessions, requestID, now, movieID).Result()
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

	return res, nil
}
