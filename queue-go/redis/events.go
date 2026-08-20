package redis

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
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
	members := make([]goredis.Z, 0, len(rids))
	for i, rid := range rids {
		id := end - int64(len(rids)-1-i)
		b, mErr := json.Marshal(FeedEvent{ID: id, T: nowMs, Type: evType, Rid: rid})
		if mErr != nil {
			continue
		}
		members = append(members, goredis.Z{Score: float64(id), Member: b})
	}
	if len(members) == 0 {
		return nil
	}
	// score = 이벤트 id. 읽는 쪽이 "id가 after보다 큰 것"을 Redis에서 바로 자르게 하려고
	//   List가 아니라 Sorted Set에 담는다. List면 조건을 서버에서 못 걸어 캡 전체를 받아
	//   앱에서 잘라야 한다 — 폴링은 대부분 "새 게 없다"라 그 전송이 통째로 낭비가 된다.
	// 캡은 ZREMRANGEBYRANK로 오래된 쪽(rank 0부터)을 잘라 유지한다.
	pipe := c.rdb.Pipeline()
	pipe.ZAdd(ctx, EventsKey(movieID), members...)
	pipe.ZRemRangeByRank(ctx, EventsKey(movieID), 0, -int64(eventCap)-1)
	pipe.Expire(ctx, EventsKey(movieID), eventTTL)
	pipe.Expire(ctx, EventsIDKey(movieID), eventTTL)
	_, err = pipe.Exec(ctx)
	return err
}

// ReadEvents = after(마지막으로 본 id) 이후의 이벤트를 오래된 것부터 반환.
// score가 id라 "(after"로 열린 하한을 줘 Redis가 잘라서 보낸다 — 새 이벤트가 없으면 0건이 온다.
// 폴링 대부분이 그 경우라, 캡 전체를 받아 앱에서 자르던 것과 전송량이 자릿수로 갈린다.
// 첫 로드(after=0)는 최근 30건만. ZSet은 오름차순이라 그때만 Rev 계열로 받고 뒤집는다.
func (c *Client) ReadEvents(ctx context.Context, movieID string, after int64) ([]FeedEvent, int64, error) {
	key := EventsKey(movieID)

	// last = 현재 피드의 최대 id. 데이터 초기화로 id가 뒤로 가면 클라이언트가 이 값으로 감지한다.
	// 비어 있으면 0.
	var last int64
	if top, err := c.rdb.ZRevRangeWithScores(ctx, key, 0, 0).Result(); err != nil {
		return nil, 0, err
	} else if len(top) > 0 {
		last = int64(top[0].Score)
	}

	var raw []string
	var err error
	if after == 0 {
		raw, err = c.rdb.ZRevRange(ctx, key, 0, 29).Result() // 최신 30건(내림차순)
	} else {
		raw, err = c.rdb.ZRangeByScore(ctx, key, &goredis.ZRangeBy{
			Min: "(" + strconv.FormatInt(after, 10), // 열린 하한 — after 자신은 뺀다
			Max: "+inf",
		}).Result() // 이미 오름차순
	}
	if err != nil {
		return nil, 0, err
	}

	out := make([]FeedEvent, 0, len(raw))
	for _, s := range raw {
		var e FeedEvent
		if json.Unmarshal([]byte(s), &e) != nil {
			continue
		}
		out = append(out, e)
	}
	if after == 0 { // ZRevRange 결과라 내림차순 — 오래된 것부터로 뒤집는다
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
	}
	return out, last, nil
}
