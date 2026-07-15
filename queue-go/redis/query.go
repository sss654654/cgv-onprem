package redis

import "context"

// 조회 헬퍼 — 승격 루프가 대상 영화·대기인원을 계산할 때 쓴다.

// WaitingCount = 현재 대기(waiting) 인원.
func (c *Client) WaitingCount(ctx context.Context, movieID string) (int64, error) {
	return c.rdb.ZCard(ctx, WaitingKey(movieID)).Result()
}

// ActiveCount = 현재 입장(active) 인원 — queue_active 게이지(설계서 §7-B)의 재료.
func (c *Client) ActiveCount(ctx context.Context, movieID string) (int64, error) {
	return c.rdb.ZCard(ctx, ActiveKey(movieID)).Result()
}

// ActiveQueueMovies = 트래픽 있는 영화 = active_movies ∪ waiting_movies (중복 제거).
func (c *Client) ActiveQueueMovies(ctx context.Context) ([]string, error) {
	a, err := c.rdb.SMembers(ctx, ActiveMoviesKey).Result()
	if err != nil {
		return nil, err
	}
	w, err := c.rdb.SMembers(ctx, WaitingMoviesKey).Result()
	if err != nil {
		return nil, err
	}
	set := make(map[string]struct{}, len(a)+len(w))
	for _, m := range a {
		set[m] = struct{}{}
	}
	for _, m := range w {
		set[m] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for m := range set {
		out = append(out, m)
	}
	return out, nil
}
