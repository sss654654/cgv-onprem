package redis

import (
	"context"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// 승격 처리율(초당 승격 수) — ETA 계산 재료.
//
// 파드 메모리 EMA로 두면 두 가지가 깨진다.
//  1) 파드마다 다른 값을 들고 있어 같은 사용자가 폴링할 때마다 ETA가 갈린다(요청이 라운드로빈).
//  2) 승격은 정원이 빌 때만 일어난다 — 정원 2·세션 60초면 60초에 한 번 몰아서 도는 주기 신호다.
//     반감기가 그 주기보다 짧은 평활은 승격 없는 구간에서 0에 수렴해, 실제로는 1분 뒤 입장할
//     사람에게 "수십 분"이 찍힌다.
//
// 그래서 승격 수를 초 단위 버킷에 기록하고 창(window) 전체를 합산해 평균을 낸다.
// 값이 Redis에 있으니 전 파드가 같은 rate를 읽고, 창이 회전 주기보다 길어 주기 신호에도 견딘다.
// 버킷 키는 TTL로 스스로 사라진다(청소 루프 불필요).

// rateWindowSec = 처리율 평균을 내는 창(초). 회전 주기(세션 60초)보다 길게 잡아야
// "승격이 몰려 있는 구간 + 비어 있는 구간"이 한 창에 같이 들어와 평균이 안정된다.
const rateWindowSec = 90

// rateBucketTTL = 개별 버킷의 수명(초). 창보다 넉넉히 둬서 창 끝의 버킷이 읽기 직전 사라지지 않게.
const rateBucketTTL = rateWindowSec + 30

// RateBucketKey = 초 단위 승격 카운터. 키 자체에 시각(초)이 들어가고 TTL로 소멸한다.
func RateBucketKey(movieID string, unixSec int64) string {
	return "promote_rate:{" + movieID + "}:" + strconv.FormatInt(unixSec, 10)
}

// ObservePromotion = 이번 틱에 promoted명을 승격했음을 현재 초 버킷에 기록.
// 승격 루프가 부른다. promoted=0이면 기록하지 않는다 — 0을 쓰면 빈 버킷이 TTL만 잡아먹고,
// 창 평균은 어차피 "창 전체 합 ÷ 창 길이"라 기록 없는 구간이 자동으로 0으로 계산된다.
func (c *Client) ObservePromotion(ctx context.Context, movieID string, promoted int, nowSec int64) error {
	if promoted <= 0 {
		return nil
	}
	key := RateBucketKey(movieID, nowSec)
	pipe := c.rdb.Pipeline()
	pipe.IncrBy(ctx, key, int64(promoted))
	pipe.Expire(ctx, key, rateBucketTTL*time.Second)
	_, err := pipe.Exec(ctx)
	return err
}

// PromotionRate = 최근 창(rateWindowSec)의 초당 승격 수.
// 창 안의 버킷을 MGET 한 번으로 읽어 합산 ÷ 창 길이. 0이면 "아직 알 수 없음".
//
// 창 전체를 나누는 이유(합계 ÷ 관측된 버킷 수가 아니라): 승격이 없던 구간도 실제로 기다린
// 시간이다. 관측된 버킷만 평균 내면 "몰아서 도는 순간의 속도"가 되어 ETA를 과소평가한다.
func (c *Client) PromotionRate(ctx context.Context, movieID string, nowSec int64) (float64, error) {
	keys := make([]string, 0, rateWindowSec)
	for i := int64(0); i < rateWindowSec; i++ {
		keys = append(keys, RateBucketKey(movieID, nowSec-i))
	}
	vals, err := c.rdb.MGet(ctx, keys...).Result()
	if err != nil && err != goredis.Nil {
		return 0, err
	}
	var total int64
	for _, v := range vals {
		s, ok := v.(string)
		if !ok {
			continue // 기록 없는 초 = 승격 0
		}
		if n, convErr := strconv.ParseInt(s, 10, 64); convErr == nil {
			total += n
		}
	}
	return float64(total) / float64(rateWindowSec), nil
}
