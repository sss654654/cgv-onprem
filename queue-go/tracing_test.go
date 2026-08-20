package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// 표본 정책이 어긋나도 요청은 정상 처리되고 에러도 안 난다 — 트레이스가 조용히 늘거나
// 줄 뿐이다. 그래서 실패를 사람이 알아채는 경로가 없어 테스트로 고정한다.
//
// 특히 위험한 방향이 "폴링이 목록에 안 잡히는 것" 이다. 그러면 전 경로가 AlwaysSample 이 되어
// 부하 판에서 Tempo 가 트레이스를 버리고 Alloy 메모리가 limit 에 붙는다(2026-08 실측).

// 실물 span 이름으로 판정되는지 본다. otelgin 은 경로 앞에 메서드를 붙이므로
// ("GET /api/admission/position") 경로만 비교하면 영원히 안 맞는다 — 그 회귀를 잡는 테스트다.
// 이름 형식을 문자열로 적어 두지 않고 otelgin 이 실제로 만든 span 을 그대로 넣어,
// 라이브러리가 형식을 바꾸면 여기서 걸리게 한다.
func TestSamplerUsesRealSpanNames(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	t.Cleanup(func() { _ = tp.Shutdown(t.Context()) })

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(otelgin.Middleware("queue-go", otelgin.WithTracerProvider(tp)))
	noop := func(c *gin.Context) { c.Status(http.StatusOK) }
	r.GET("/api/admission/position", noop)
	r.GET("/api/admission/stats", noop)
	r.GET("/api/admission/events", noop)
	r.GET("/health/ready", noop)
	r.POST("/api/admission/enter", noop)
	r.POST("/api/admission/complete", noop)

	// 폴링이냐 아니냐 = 표본에서 깎이느냐. 값을 확실히 가르려고 양쪽 극단을 쓴다.
	sampler := byRoute{polling: sdktrace.NeverSample(), rest: sdktrace.AlwaysSample()}

	cases := []struct {
		method, path string
		wantSampled  bool
	}{
		{http.MethodGet, "/api/admission/position", false}, // 대기 화면이 1-5초마다
		{http.MethodGet, "/api/admission/stats", false},    // 현황 타일 3초
		{http.MethodGet, "/api/admission/events", false},   // 실황 피드 3초
		{http.MethodGet, "/health/ready", false},           // kubelet 프로브
		{http.MethodPost, "/api/admission/enter", true},    // 사용자당 1회
		{http.MethodPost, "/api/admission/complete", true}, // 사용자당 1회
	}

	for _, c := range cases {
		before := len(rec.Ended())
		r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(c.method, c.path, nil))
		ended := rec.Ended()
		if len(ended) != before+1 {
			t.Fatalf("%s %s: span 이 하나 생겨야 한다 (before=%d after=%d)", c.method, c.path, before, len(ended))
		}
		name := ended[len(ended)-1].Name()

		got := sampler.ShouldSample(sdktrace.SamplingParameters{Name: name}).Decision
		gotSampled := got == sdktrace.RecordAndSample
		if gotSampled != c.wantSampled {
			t.Errorf("span %q: 표본 유지=%v, 기대=%v (routeOf=%q)", name, gotSampled, c.wantSampled, routeOf(name))
		}
	}
}

// 배경 루프가 여는 루트 span. 이쪽이 깎이면 승격 → admissions → booking 체인이 끊긴다.
func TestBackgroundSpansAlwaysSampled(t *testing.T) {
	sampler := byRoute{polling: sdktrace.NeverSample(), rest: sdktrace.AlwaysSample()}
	for _, name := range []string{
		"promote cycle",
		"publish admissions",
		"publish admissions-revoked",
		"consume bookings-completed",
		"GET", // 라우트가 안 잡힌 요청 — otelgin 은 메서드만 남긴다
	} {
		if d := sampler.ShouldSample(sdktrace.SamplingParameters{Name: name}).Decision; d != sdktrace.RecordAndSample {
			t.Errorf("span %q 는 전부 남아야 한다, got %v", name, d)
		}
	}
}

// 부모 없이 뜨는 Redis 명령 span. 배경 루프가 일 없이 도는 조회라 트레이스로 남길 값이 없고,
// 안 깎으면 유휴에도 초당 열 개 넘게 쌓인다(실측 11.37 span/초).
// 같은 명령이라도 요청·승격 안에서 불리면 부모가 있어 ParentBased 가 먼저 판정하므로
// 이 함수까지 오지 않는다 — 즉 "루트인 Client" 만 걸린다.
func TestOrphanClientSpansSampledDown(t *testing.T) {
	sampler := byRoute{polling: sdktrace.NeverSample(), rest: sdktrace.AlwaysSample()}
	for _, name := range []string{"evalsha", "zcard", "get", "pipeline"} {
		p := sdktrace.SamplingParameters{Name: name, Kind: trace.SpanKindClient}
		if d := sampler.ShouldSample(p).Decision; d != sdktrace.Drop {
			t.Errorf("고아 Client span %q 는 깎여야 한다, got %v", name, d)
		}
	}
	// Internal 은 우리가 연 사이클 span 이라 남아야 한다.
	p := sdktrace.SamplingParameters{Name: "promote cycle", Kind: trace.SpanKindInternal}
	if d := sampler.ShouldSample(p).Decision; d != sdktrace.RecordAndSample {
		t.Errorf("사이클 span 은 남아야 한다, got %v", d)
	}
}
