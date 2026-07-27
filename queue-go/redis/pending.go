package redis

import (
	"context"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// 발행 대기 저널(PendingKey)의 member 어휘.
// member = "<kind>|<reason>|<requestId>". requestId는 핸들러에서 [A-Za-z0-9_-]로 제한하므로
// 구분자 '|'가 값에 섞이지 않는다.
const (
	KindAdmit  = "A" // booking에 "입장했다"를 알려야 하는 상태 → admissions 토픽
	KindRevoke = "R" // booking에 "입장이 끝났다"를 알려야 하는 상태 → admissions-revoked 토픽

	ReasonEnter          = "ENTER"           // 정원 여유로 즉시 입장
	ReasonPromote        = "PROMOTE"         // 대기열에서 승격
	ReasonSessionTimeout = "SESSION_TIMEOUT" // active 세션 수명 초과로 회수
	ReasonLeave          = "LEAVE"           // 사용자가 대기열/입장에서 이탈
	ReasonComplete       = "COMPLETE"        // 사용자가 자리 반환(complete)
)

// PendingEvent = 저널 한 줄을 파싱한 것.
type PendingEvent struct {
	Kind      string
	Reason    string
	RequestID string
	Member    string // 저널에 실제로 들어있는 원문(제거 시 그대로 ZREM)
}

// PendingMember는 저널 member 문자열을 만든다.
func PendingMember(kind, reason, requestID string) string {
	return kind + "|" + reason + "|" + requestID
}

// ParsePendingMember는 member를 3조각으로 나눈다. 형식이 아니면 ok=false.
func ParsePendingMember(m string) (PendingEvent, bool) {
	parts := strings.SplitN(m, "|", 3)
	if len(parts) != 3 || parts[2] == "" {
		return PendingEvent{}, false
	}
	return PendingEvent{Kind: parts[0], Reason: parts[1], RequestID: parts[2], Member: m}, true
}

// PendingBefore = score(상태변경시각)가 cutoff 이하인 저널 항목을 오래된 순으로 최대 limit개.
// cutoff에 유예를 두는 이유: 정상 경로는 상태변경 직후 발행하고 저널을 지우므로,
// 유예 안의 항목은 "지금 발행 중"일 뿐이다. 유예를 지나도 남아 있으면 발행이 끝나지 못한 것.
func (c *Client) PendingBefore(ctx context.Context, movieID string, cutoff, limit int64) ([]PendingEvent, error) {
	raw, err := c.rdb.ZRangeByScore(ctx, PendingKey(movieID), &goredis.ZRangeBy{
		Min:   "-inf",
		Max:   strconv.FormatInt(cutoff, 10),
		Count: limit,
	}).Result()
	if err != nil {
		return nil, err
	}
	out := make([]PendingEvent, 0, len(raw))
	for _, m := range raw {
		if e, ok := ParsePendingMember(m); ok {
			out = append(out, e)
		} else {
			// 형식 불명 member는 되돌릴 방법이 없어 저널에 영원히 남는다 → 즉시 제거.
			c.rdb.ZRem(ctx, PendingKey(movieID), m)
		}
	}
	return out, nil
}

// ClearPending = 발행에 성공한 저널 항목 제거.
func (c *Client) ClearPending(ctx context.Context, movieID string, members []string) error {
	if len(members) == 0 {
		return nil
	}
	args := make([]interface{}, len(members))
	for i, m := range members {
		args[i] = m
	}
	return c.rdb.ZRem(ctx, PendingKey(movieID), args...).Err()
}

// PendingCount = 저널에 쌓인 미발행 항목 수(지표 재료 — 자라면 Kafka 발행이 막힌 것).
func (c *Client) PendingCount(ctx context.Context, movieID string) (int64, error) {
	return c.rdb.ZCard(ctx, PendingKey(movieID)).Result()
}

// SetPromotePause = 발행 실패 시 승격을 d 동안 중단(전 파드 공통). TTL이 지나면 자동 재개.
func (c *Client) SetPromotePause(ctx context.Context, d time.Duration) error {
	return c.rdb.Set(ctx, PromotePauseKey, "1", d).Err()
}

// PromotePaused = 지금 승격 중단 구간인가.
func (c *Client) PromotePaused(ctx context.Context) (bool, error) {
	n, err := c.rdb.Exists(ctx, PromotePauseKey).Result()
	return n > 0, err
}
