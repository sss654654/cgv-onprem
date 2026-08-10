package redis

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

// 정원을 지키는 주체는 Go 코드가 아니라 Redis Lua 스크립트다 — "정원 확인"과 "입장 등록"이
// 한 원자 실행 안에 있어야 동시 요청이 그 틈을 벌리지 못한다. 그래서 이 테스트는 가짜 저장소가
// 아니라 실제 Redis에 대고 돌린다. 가짜로 흉내 내면 흉내 낸 자기 자신을 검증하게 된다.
//
// 순차로 부르는 검사는 어떤 구현이든 통과하므로 회귀를 잡지 못한다. 동시에 출발시켜야 한다.

// newTestClient = 테스트용 Redis 연결. 닿지 않으면 실패가 아니라 건너뛴다 —
// Redis 없이 `go test ./...`를 돌리는 개발 중 실행이 빨간불이 되지 않게.
// CI에서는 서비스 컨테이너가 붙어 있어 항상 실행된다.
func newTestClient(t *testing.T) *Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	c := New(addr, os.Getenv("REDIS_PASSWORD"), 0, "", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Ping(ctx); err != nil {
		t.Skipf("Redis(%s)에 닿지 않아 건너뛴다: %v", addr, err)
	}
	return c
}

// 영화 id를 실행마다 다르게 만든다 — 같은 Redis를 여러 테스트가 함께 쓰더라도 키가 겹치지 않는다.
func newTestMovieID(prefix string) string {
	return prefix + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
}

// 동시에 들어와도 입장 인원이 정원을 넘지 않는다.
func TestEnterKeepsCapacityUnderConcurrency(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	movieID := newTestMovieID("test-capacity")
	t.Cleanup(func() { _ = c.Reset(ctx, movieID, time.Now().Unix()) })

	const capacity = 3
	const callers = 50

	results := make([]EnterResult, callers)
	errs := make([]error, callers)
	start := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // 전원을 같은 순간에 출발시킨다
			results[i], errs[i] = c.Enter(ctx, movieID, fmt.Sprintf("tester-%d", i), capacity, time.Now().UnixMilli())
		}(i)
	}
	close(start)
	wg.Wait()

	admitted, waiting := 0, 0
	for i, r := range results {
		if errs[i] != nil {
			t.Fatalf("입장 요청이 실패했다: %v", errs[i])
		}
		switch r.Code {
		case "ADMITTED":
			admitted++
		case "WAITING":
			waiting++
		default:
			t.Fatalf("예상 밖 응답 코드: %s", r.Code)
		}
	}

	if admitted != capacity {
		t.Errorf("입장 인원이 정원과 다르다: 정원 %d, 입장 %d", capacity, admitted)
	}
	if waiting != callers-capacity {
		t.Errorf("대기 인원이 맞지 않는다: 기대 %d, 실제 %d", callers-capacity, waiting)
	}

	// 응답만 세지 않고 저장된 명부도 확인한다 — 응답이 맞아도 명부가 넘칠 수 있다.
	stored, err := c.ActiveCount(ctx, movieID)
	if err != nil {
		t.Fatalf("입장 인원 조회에 실패했다: %v", err)
	}
	if stored != capacity {
		t.Errorf("저장된 입장 인원이 정원을 넘었다: %d(정원 %d)", stored, capacity)
	}
}

// 같은 신원이 다시 들어와도 자리가 늘지 않는다 — 새로고침만으로 정원이 뚫리지 않게.
func TestEnterIsIdempotentForSameRequestID(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	movieID := newTestMovieID("test-idempotent")
	t.Cleanup(func() { _ = c.Reset(ctx, movieID, time.Now().Unix()) })

	const capacity = 2
	const requestID = "same-visitor"

	first, err := c.Enter(ctx, movieID, requestID, capacity, time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("첫 입장 요청이 실패했다: %v", err)
	}
	if first.Code != "ADMITTED" {
		t.Fatalf("정원이 비어 있는데 입장하지 못했다: %s", first.Code)
	}

	second, err := c.Enter(ctx, movieID, requestID, capacity, time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("재진입 요청이 실패했다: %v", err)
	}
	if second.Code != "ALREADY_ACTIVE" {
		t.Errorf("재진입이 새 입장으로 처리됐다: %s", second.Code)
	}

	stored, err := c.ActiveCount(ctx, movieID)
	if err != nil {
		t.Fatalf("입장 인원 조회에 실패했다: %v", err)
	}
	if stored != 1 {
		t.Errorf("재진입으로 자리가 늘었다: %d명", stored)
	}
}
