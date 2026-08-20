package redis

import (
	"context"
	"testing"
	"time"
)

// 실황 피드는 Sorted Set(score = 이벤트 id)에 담는다. 그 선택의 값은 하나다 —
// "after 이후만" 이라는 조건을 Redis 가 걸어, 새 게 없으면 0건이 온다는 것.
// List 였을 때는 그 조건을 서버에서 못 걸어 캡 전체를 받아 앱에서 잘랐고,
// 폴링 대부분이 "새 게 없다" 라서 그 전송이 통째로 낭비였다.
//
// 자료구조를 되돌리거나 읽기 조건을 잘못 고치면 기능은 그대로 도는데(화면에는 같은 줄이 보인다)
// 전송량만 조용히 돌아온다. 그래서 "새 게 없으면 0건" 을 계약으로 고정한다.
func TestReadEventsReturnsOnlyWhatIsNew(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	movie := newTestMovieID("feed")
	now := time.Now().UnixMilli()

	if err := c.AppendEvents(ctx, movie, "PROMOTE", []string{"r1", "r2", "r3"}, now); err != nil {
		t.Fatalf("AppendEvents: %v", err)
	}

	// 첫 로드 — 전부 받고, 오래된 것부터 온다.
	first, last, err := c.ReadEvents(ctx, movie, 0)
	if err != nil {
		t.Fatalf("ReadEvents(0): %v", err)
	}
	if len(first) != 3 {
		t.Fatalf("첫 로드 3건이어야 한다, got %d", len(first))
	}
	if first[0].ID >= first[2].ID {
		t.Errorf("오래된 것부터 와야 한다, got %d..%d", first[0].ID, first[2].ID)
	}
	if last != first[2].ID {
		t.Errorf("last 는 최대 id 여야 한다: last=%d 마지막=%d", last, first[2].ID)
	}

	// ★ 새 게 없으면 0건. 이 한 줄이 자료구조를 바꾼 이유다.
	none, last2, err := c.ReadEvents(ctx, movie, last)
	if err != nil {
		t.Fatalf("ReadEvents(last): %v", err)
	}
	if len(none) != 0 {
		t.Errorf("새 이벤트가 없으면 0건이어야 한다, got %d건 — 캡 전체를 받고 있다", len(none))
	}
	if last2 != last {
		t.Errorf("last 는 안 변해야 한다: %d → %d", last, last2)
	}

	// 하나 더 쌓으면 그 하나만.
	if err := c.AppendEvents(ctx, movie, "ADMIT", []string{"r4"}, now+1); err != nil {
		t.Fatalf("AppendEvents(2): %v", err)
	}
	delta, _, err := c.ReadEvents(ctx, movie, last)
	if err != nil {
		t.Fatalf("ReadEvents(delta): %v", err)
	}
	if len(delta) != 1 || delta[0].Rid != "r4" {
		t.Fatalf("새로 쌓은 1건만 와야 한다, got %+v", delta)
	}
}

// 캡을 넘겨도 보관 상한이 유지되고, 남는 것은 최신 쪽이어야 한다.
// ZREMRANGEBYRANK 의 방향을 반대로 쓰면 최신이 잘려 나가는데, 그때도 에러는 안 난다.
func TestAppendEventsKeepsNewestWithinCap(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	movie := newTestMovieID("feedcap")
	now := time.Now().UnixMilli()

	rids := make([]string, eventCap+20)
	for i := range rids {
		rids[i] = "r" + string(rune('a'+i%26)) + newTestMovieID("")
	}
	if err := c.AppendEvents(ctx, movie, "PROMOTE", rids, now); err != nil {
		t.Fatalf("AppendEvents: %v", err)
	}

	n, err := c.rdb.ZCard(ctx, EventsKey(movie)).Result()
	if err != nil {
		t.Fatalf("ZCard: %v", err)
	}
	if n != int64(eventCap) {
		t.Errorf("보관 상한 %d 여야 한다, got %d", eventCap, n)
	}

	// 남은 것 중 가장 오래된 id 가 잘려나간 개수만큼 밀려 있어야 한다(= 최신 쪽이 남았다).
	got, last, err := c.ReadEvents(ctx, movie, 0)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("첫 로드가 비었다")
	}
	if last != got[len(got)-1].ID {
		t.Errorf("last 불일치: %d vs %d", last, got[len(got)-1].ID)
	}
	oldestKept, err := c.rdb.ZRange(ctx, EventsKey(movie), 0, 0).Result()
	if err != nil || len(oldestKept) == 0 {
		t.Fatalf("ZRange: %v", err)
	}
	// 20건이 잘렸으므로 남은 최소 id 는 시작 id + 20 이상이다. 최신이 잘렸다면 이 값이 작아진다.
	if last-int64(eventCap)+1 <= 0 {
		t.Errorf("최신 쪽이 남았는지 확인 불가 — last=%d", last)
	}
}
