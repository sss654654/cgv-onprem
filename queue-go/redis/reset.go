package redis

import (
	"context"
)

// Reset = 한 영화의 대기열 상태를 전부 지운다(운영용). 데모 데이터가 쌓였을 때 되돌리는 경로.
//
// 지우는 것: active·waiting·생존추적·누적승격·발행 대기 저널·처리율 버킷,
//            그리고 추적 Set(active_movies·waiting_movies)의 이 영화 항목.
// 남기는 것: booking 쪽 예매 기록·좌석 락(별도 서비스 소관 — booking의 리셋이 맡는다).
//
// 처리율 버킷은 키에 시각(초)이 들어가 개수가 정해져 있지 않다. SCAN으로 훑는 대신
// 창 범위(+TTL 여유)의 키를 계산해서 지운다 — SCAN은 커서가 DB 전체 키를 훑는데,
// 이 Redis는 대기열 폴링을 처리하는 바로 그 인스턴스다.
func (c *Client) Reset(ctx context.Context, movieID string, nowSec int64) error {
	keys := []string{
		ActiveKey(movieID),
		WaitingKey(movieID),
		WaitingLastseenKey(movieID),
		PromotedCountKey(movieID),
		PendingKey(movieID),
	}
	for i := int64(0); i < rateBucketTTL; i++ {
		keys = append(keys, RateBucketKey(movieID, nowSec-i))
	}
	pipe := c.rdb.Pipeline()
	pipe.Del(ctx, keys...)
	pipe.SRem(ctx, ActiveMoviesKey, movieID)
	pipe.SRem(ctx, WaitingMoviesKey, movieID)
	pipe.Del(ctx, PromotePauseKey)
	_, err := pipe.Exec(ctx)
	return err
}
