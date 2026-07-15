package redis

import (
	"context"
	"log/slog"

	goredis "github.com/redis/go-redis/v9"
)

// PositionResult = GET /position 응답 재료(백엔드서비스-올인원.md §1-2).
type PositionResult struct {
	Status   string // WAITING / ADMITTED / EXPIRED
	Position int64  // 1-based 순번(WAITING일 때만)
	Behind   int64  // 내 뒤 인원(WAITING일 때만)
}

// Position = 폴링 순번 조회(§1-2). 판정 순서가 중요:
//  ① ZSCORE active  → 있으면 ADMITTED (승격됨 — active 먼저 봐야 승격자를 놓치지 않음)
//  ② ZRANK waiting  → 있으면 WAITING + position·behind + lastseen 도장
//  ③ 둘 다 없음     → EXPIRED (타임아웃·이탈·완료)
// [2-1] 로컬은 캐시 없이 매 요청 ZRANK 직접(부하 없음). 캐싱은 2-2에서.
func (c *Client) Position(ctx context.Context, movieID, requestID string, now int64) (PositionResult, error) {
	// ① ADMITTED (직접 조회 — 캐시 지연으로 입장 늦으면 안 됨, §1-2)
	if _, err := c.rdb.ZScore(ctx, ActiveKey(movieID), requestID).Result(); err == nil {
		return PositionResult{Status: "ADMITTED"}, nil
	} else if err != goredis.Nil {
		return PositionResult{}, err
	}

	// ② WAITING
	rank, err := c.rdb.ZRank(ctx, WaitingKey(movieID), requestID).Result()
	if err == goredis.Nil {
		return PositionResult{Status: "EXPIRED"}, nil // ③
	}
	if err != nil {
		return PositionResult{}, err
	}
	total, err := c.rdb.ZCard(ctx, WaitingKey(movieID)).Result()
	if err != nil {
		return PositionResult{}, err
	}
	position := rank + 1        // ZRANK 0-based → 1-based
	behind := total - position  // = total − rank − 1 (오프바이원 주의, §1-2)
	if behind < 0 {
		behind = 0
	}

	// 생존 도장 갱신(§1-4b) — 폴링이 곧 "나 살아있음" 신호.
	// 실패 로그 필수: 이 도장이 조용히 실패하면 폴링 중인 유저를 타임아웃이 쫓아낸다.
	if err := c.rdb.ZAdd(ctx, WaitingLastseenKey(movieID), goredis.Z{Score: float64(now), Member: requestID}).Err(); err != nil {
		slog.WarnContext(ctx, "position: lastseen 도장 갱신 실패", "movie", movieID, "req", requestID, "err", err)
	}

	return PositionResult{Status: "WAITING", Position: position, Behind: behind}, nil
}
