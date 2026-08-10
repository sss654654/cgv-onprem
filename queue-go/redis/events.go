package redis

import (
	"context"
	"encoding/json"
	"time"
)

// 이벤트 피드 — 방문자 화면의 실황 재료. 운영 기록이 아니다(그건 stdout 로그 몫).
// 최근 소량만 캡으로 유지해, 어느 화면이든 "지금 무슨 일이 벌어지나"를 같은 내용으로 본다.
// 값이 Redis에 있으므로 새로고침·새 탭·다른 기기에서도 피드가 이어진다.
//
// 쓰기는 전부 best-effort다: 피드는 표시용이라, 기록 실패가 입장·승격 같은
// 본 연산을 실패시키면 주객이 바뀐다. 호출부는 에러를 로그로만 남긴다.

const (
	eventCap = 100            // 보관 상한 — 화면이 보여줄 만큼만
	eventTTL = 24 * time.Hour // 유휴 영화의 피드는 하루 뒤 자멸
)

func EventsKey(movieID string) string   { return "events:{" + movieID + "}" }
func EventsIDKey(movieID string) string { return "events_id:{" + movieID + "}" }

// FeedEvent — id는 영화별 단조 증가(INCRBY 발급). 클라이언트가 "어디까지 봤나"를 id로
// 가른다. 시각만으로 가르면 같은 밀리초의 이벤트가 중복 표시되거나 빠진다.
type FeedEvent struct {
	ID   int64  `json:"id"`
	T    int64  `json:"t"`    // unix ms
	Type string `json:"type"` // ADMIT·PROMOTE·SESSION_EXPIRE·WAIT_EXPIRE·LEAVE·RETURN
	Rid  string `json:"rid"`
}

// AppendEvents = 같은 종류의 이벤트 n건을 한 번에 기록(승격 배치 대응).
func (c *Client) AppendEvents(ctx context.Context, movieID, evType string, rids []string, nowMs int64) error {
	if len(rids) == 0 {
		return nil
	}
	// id를 n개 몫으로 한 번에 발급 — 시작 id = end-n+1
	end, err := c.rdb.IncrBy(ctx, EventsIDKey(movieID), int64(len(rids))).Result()
	if err != nil {
		return err
	}
	vals := make([]interface{}, 0, len(rids))
	for i, rid := range rids {
		b, mErr := json.Marshal(FeedEvent{ID: end - int64(len(rids)-1-i), T: nowMs, Type: evType, Rid: rid})
		if mErr != nil {
			continue
		}
		vals = append(vals, b)
	}
	if len(vals) == 0 {
		return nil
	}
	// LPUSH는 넘긴 순서대로 왼쪽에 쌓이므로 마지막 원소(최신 id)가 리스트 머리가 된다.
	pipe := c.rdb.Pipeline()
	pipe.LPush(ctx, EventsKey(movieID), vals...)
	pipe.LTrim(ctx, EventsKey(movieID), 0, eventCap-1)
	pipe.Expire(ctx, EventsKey(movieID), eventTTL)
	pipe.Expire(ctx, EventsIDKey(movieID), eventTTL)
	_, err = pipe.Exec(ctx)
	return err
}

// ReadEvents = after(마지막으로 본 id) 이후의 이벤트를 오래된 것부터 반환.
// after=0(첫 로드)이면 최근 30건만 — 화면을 처음부터 100줄로 덮지 않는다.
// last = 현재 피드의 최대 id. 데이터 초기화로 id가 뒤로 가면 클라이언트가 이 값으로 감지한다.
func (c *Client) ReadEvents(ctx context.Context, movieID string, after int64) ([]FeedEvent, int64, error) {
	raw, err := c.rdb.LRange(ctx, EventsKey(movieID), 0, eventCap-1).Result()
	if err != nil {
		return nil, 0, err
	}
	out := make([]FeedEvent, 0, len(raw))
	var last int64
	for _, s := range raw { // 리스트는 새 것부터 — id 내림차순
		var e FeedEvent
		if json.Unmarshal([]byte(s), &e) != nil {
			continue
		}
		if e.ID > last {
			last = e.ID
		}
		if e.ID <= after {
			break // 내림차순이라 이후는 전부 이미 본 것
		}
		out = append(out, e)
	}
	if after == 0 && len(out) > 30 {
		out = out[:30]
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 { // 오래된 것부터로 뒤집기
		out[i], out[j] = out[j], out[i]
	}
	return out, last, nil
}
