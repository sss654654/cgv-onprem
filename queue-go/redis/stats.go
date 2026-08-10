package redis

import (
	"context"
	"errors"

	goredis "github.com/redis/go-redis/v9"
)

// Stats = 화면 표시용 집계(읽기 전용) — 현재 입장·대기 인원과 누적 승격 수.
// 셋 다 이미 Redis에 사는 값이다(active·waiting ZSET, promoted_count 카운터).
// 파이프라인 한 왕복으로 묶는다 — 폴링 주기로 불리는 경로라 왕복 수가 곧 부하.
func (c *Client) Stats(ctx context.Context, movieID string) (active, waiting, promoted int64, err error) {
	pipe := c.rdb.Pipeline()
	aCmd := pipe.ZCard(ctx, ActiveKey(movieID))
	wCmd := pipe.ZCard(ctx, WaitingKey(movieID))
	pCmd := pipe.Get(ctx, PromotedCountKey(movieID))
	// promoted_count 키는 첫 승격 전엔 없다 — GET의 Nil은 0으로 취급, 에러가 아니다.
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
		return 0, 0, 0, err
	}
	promoted, _ = pCmd.Int64()
	return aCmd.Val(), wCmd.Val(), promoted, nil
}
