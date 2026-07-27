package redis

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

// PositionResult = GET /position 응답 재료(백엔드서비스-올인원.md §1-2).
type PositionResult struct {
	Status   string // WAITING / ADMITTED / EXPIRED
	Position int64  // 1-based 순번(WAITING일 때만)
	Behind   int64  // 내 뒤 인원(WAITING일 때만)
}

// positionScript = 3-state 판정 + 생존 도장을 한 번의 원자 실행으로(§1-2).
//  ① ZSCORE active  → 있으면 ADMITTED (승격됨 — active 먼저 봐야 승격자를 놓치지 않음)
//  ② ZRANK waiting  → 있으면 WAITING + position·behind + lastseen 도장
//  ③ 둘 다 없음     → EXPIRED (타임아웃·이탈·완료)
//
// Lua 한 덩어리로 묶은 이유: ZSCORE와 ZRANK를 따로 왕복하면 그 사이에 승격 Lua가 끼어
// waiting에서 빠진 사람을 "둘 다 없음"으로 읽는다 → 방금 입장한 사용자가 EXPIRED를 받아
// 화면에서 자리를 잃고, 그 자리는 세션 타임아웃까지 아무도 못 쓴다. 판정 순서만으로는
// 그 창이 안 닫히고, 원자 실행이라야 닫힌다. 왕복도 최대 3회에서 1회로 준다(폴링 = 최다 호출).
//   KEYS[1]=active, KEYS[2]=waiting, KEYS[3]=waiting_lastseen
//   ARGV[1]=member(requestId), ARGV[2]=now(ms)
//   반환 = {status, position, behind}
var positionScript = goredis.NewScript(`
local m = ARGV[1]
local now = tonumber(ARGV[2])
if redis.call('ZSCORE', KEYS[1], m) then
  return {'ADMITTED', 0, 0}
end
local rank = redis.call('ZRANK', KEYS[2], m)
if not rank then
  return {'EXPIRED', 0, 0}
end
local total = redis.call('ZCARD', KEYS[2])
local position = rank + 1
local behind = total - position
if behind < 0 then behind = 0 end
-- 생존 도장(§1-4b) — 폴링이 곧 "나 살아있음" 신호. 판정과 같은 실행 안에서 찍어야
-- 도장과 판정 사이에 waiting 타임아웃이 끼어들지 않는다.
redis.call('ZADD', KEYS[3], now, m)
return {'WAITING', position, behind}
`)

// Position = 폴링 순번 조회(§1-2).
// [2-1] 로컬은 캐시 없이 매 요청 직접 조회(부하 없음). 캐싱은 2-2에서.
func (c *Client) Position(ctx context.Context, movieID, requestID string, now int64) (PositionResult, error) {
	keys := []string{ActiveKey(movieID), WaitingKey(movieID), WaitingLastseenKey(movieID)}
	raw, err := positionScript.Run(ctx, c.rdb, keys, requestID, now).Result()
	if err != nil {
		return PositionResult{}, err
	}
	arr, ok := raw.([]interface{})
	if !ok || len(arr) < 3 {
		return PositionResult{}, fmt.Errorf("unexpected position result: %v", raw)
	}
	status, _ := arr[0].(string)
	position, _ := arr[1].(int64)
	behind, _ := arr[2].(int64)
	return PositionResult{Status: status, Position: position, Behind: behind}, nil
}
