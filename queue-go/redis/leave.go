package redis

import (
	"context"

	goredis "github.com/redis/go-redis/v9"
)

// leaveScript = 대기열에서 완전 이탈(active·waiting·waiting_lastseen 전부 제거). 멱등(ZREM은 없으면 0).
// 사용자가 대기 중 취소/홈 버튼 등으로 빠질 때. 어디 있는지 모르니 다 제거.
// waiting 제거 시 waiting_lastseen도 같이.
//
// active에 있던 사람이면 booking의 admitted에도 그 사람이 들어 있다 → 회수 이벤트를 발행
// 대기 저널에 남긴다. 안 남기면 자리를 잃은 사용자가 계속 예매 API를 통과한다.
// 아직 발행 못 한 입장 통보가 있으면 같이 지운다 — 안 지우면 sweep이 "입장했다"를 뒤늦게
// 발행해 방금 회수한 인증이 되살아난다.
//   KEYS[1]=active, KEYS[2]=waiting, KEYS[3]=waiting_lastseen, KEYS[4]=pending_events
//   ARGV[1]=member(requestId), ARGV[2]=now(ms), ARGV[3]=회수 사유
//   반환 = active에 있었으면 1
var leaveScript = goredis.NewScript(`
local m = ARGV[1]
local now = tonumber(ARGV[2])
local reason = ARGV[3]
local wasActive = redis.call('ZREM', KEYS[1], m)
redis.call('ZREM', KEYS[2], m)
redis.call('ZREM', KEYS[3], m)
redis.call('ZREM', KEYS[4], 'A|ENTER|' .. m, 'A|PROMOTE|' .. m)
if wasActive == 1 then
  redis.call('ZADD', KEYS[4], now, 'R|' .. reason .. '|' .. m)
end
return wasActive
`)

// releaseScript = active 세션만 종료(자리 반환). waiting은 건드리지 않는다.
// 나머지 규칙은 leaveScript와 같다.
//   KEYS[1]=active, KEYS[2]=pending_events
//   ARGV[1]=member, ARGV[2]=now(ms), ARGV[3]=회수 사유
//   반환 = active에 있었으면 1
var releaseScript = goredis.NewScript(`
local m = ARGV[1]
local now = tonumber(ARGV[2])
local reason = ARGV[3]
local wasActive = redis.call('ZREM', KEYS[1], m)
redis.call('ZREM', KEYS[2], 'A|ENTER|' .. m, 'A|PROMOTE|' .. m)
if wasActive == 1 then
  redis.call('ZADD', KEYS[2], now, 'R|' .. reason .. '|' .. m)
end
return wasActive
`)

// Leave = 대기열에서 완전 이탈. active에 있었으면 회수 이벤트를 저널에 남긴다.
func (c *Client) Leave(ctx context.Context, movieID, requestID string, now int64) error {
	keys := []string{ActiveKey(movieID), WaitingKey(movieID), WaitingLastseenKey(movieID), PendingKey(movieID)}
	return leaveScript.Run(ctx, c.rdb, keys, requestID, now, ReasonLeave).Err()
}

// ReleaseActive = 사용자가 부른 active 종료(자리 반환) → 회수 이벤트를 저널에 남긴다.
// 반환값 = 실제로 active에 있었는지.
func (c *Client) ReleaseActive(ctx context.Context, movieID, requestID, reason string, now int64) (bool, error) {
	keys := []string{ActiveKey(movieID), PendingKey(movieID)}
	n, err := releaseScript.Run(ctx, c.rdb, keys, requestID, now, reason).Int64()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// CompleteActive = booking의 bookings-completed 수신에 따른 자리 반환(active에서만 제거).
// 회수 이벤트는 남기지 않는다 — booking은 예매 확정 시 자기 admitted를 이미 지웠고,
// 여기서 또 회수를 발행하면 이미 끝난 건에 대한 왕복만 는다.
// 아직 발행 못 한 입장 통보는 지운다(뒤늦은 재발행으로 인증이 되살아나지 않게).
func (c *Client) CompleteActive(ctx context.Context, movieID, requestID string) (bool, error) {
	pipe := c.rdb.TxPipeline()
	rem := pipe.ZRem(ctx, ActiveKey(movieID), requestID)
	pipe.ZRem(ctx, PendingKey(movieID),
		PendingMember(KindAdmit, ReasonEnter, requestID),
		PendingMember(KindAdmit, ReasonPromote, requestID))
	if _, err := pipe.Exec(ctx); err != nil {
		return false, err
	}
	return rem.Val() > 0, nil
}
