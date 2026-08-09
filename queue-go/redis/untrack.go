package redis

import (
	"context"

	goredis "github.com/redis/go-redis/v9"
)

// untrackScript = 흔적이 없는 영화를 추적 Set에서 제거(원자).
// active_movies는 enter에서 SADD만 되던 구조라 트래픽이 끝난 영화도 영구히 남고,
// 루프 4종(승격·세션 타임아웃·폴링 타임아웃·스윕)이 매 틱 그 영화를 계속 돈다.
// waiting_movies도 promote가 비웠을 때만 SREM이라 evict로 비면 남는다 — 같이 지운다.
//
// 제거 조건 = active·waiting·발행 저널(pending_events) 셋 다 빈 것.
// 저널이 남았는데 지우면 스윕이 대상 영화를 못 찾아 재발행이 멈춘다.
// waiting이 비면 waiting_lastseen도 비어 있다(두 키는 같은 원자 실행에서만 늘고 준다).
//
// "확인 → 제거" 사이에 새 enter가 끼는 경합: enter의 추적 등록이 멤버 등록과 같은
// Lua 원자 실행이고 이 스크립트도 원자라, 둘은 직렬로만 실행된다 — 멤버가 있는데
// 추적만 사라지는 순서는 없다(제거가 먼저면 다음 enter가 다시 등록한다).
//   KEYS[1]=active, KEYS[2]=waiting, KEYS[3]=pending_events,
//   KEYS[4]=active_movies, KEYS[5]=waiting_movies
//   ARGV[1]=movieId
//   반환 = 1 제거 / 0 흔적이 남아 유지
var untrackScript = goredis.NewScript(`
if redis.call('ZCARD', KEYS[1]) == 0
  and redis.call('ZCARD', KEYS[2]) == 0
  and redis.call('ZCARD', KEYS[3]) == 0 then
  redis.call('SREM', KEYS[4], ARGV[1])
  redis.call('SREM', KEYS[5], ARGV[1])
  return 1
end
return 0
`)

// UntrackIfIdle = active·waiting·발행 저널이 전부 빈 영화를 추적 Set에서 내린다.
func (c *Client) UntrackIfIdle(ctx context.Context, movieID string) (bool, error) {
	keys := []string{ActiveKey(movieID), WaitingKey(movieID), PendingKey(movieID), ActiveMoviesKey, WaitingMoviesKey}
	n, err := untrackScript.Run(ctx, c.rdb, keys, movieID).Int64()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}
