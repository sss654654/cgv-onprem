package redis

import "context"

// Leave = 대기열에서 완전 이탈(active·waiting·waiting_lastseen 전부 제거). 멱등(ZREM은 없으면 0).
// 사용자가 대기 중 취소/홈 버튼 등으로 빠질 때. 어디 있는지 모르니 다 제거.
// waiting 제거 시 waiting_lastseen도 같이(§1-3 정합 규칙).
func (c *Client) Leave(ctx context.Context, movieID, requestID string) error {
	pipe := c.rdb.TxPipeline()
	pipe.ZRem(ctx, ActiveKey(movieID), requestID)
	pipe.ZRem(ctx, WaitingKey(movieID), requestID)
	pipe.ZRem(ctx, WaitingLastseenKey(movieID), requestID)
	_, err := pipe.Exec(ctx)
	return err
}

// CompleteActive = active 세션 종료 → 자리 반환(active에서만 제거).
// 예매 완료 시 booking이 Kafka completed로 알려 queue가 이걸 호출 → ③이 다음 대기자 승격.
func (c *Client) CompleteActive(ctx context.Context, movieID, requestID string) (bool, error) {
	n, err := c.rdb.ZRem(ctx, ActiveKey(movieID), requestID).Result()
	return n > 0, err
}
