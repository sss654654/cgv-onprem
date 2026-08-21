package main

import (
	"context"
	"os"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// neverSampled = 트레이스를 생성하지 않을 경로.
// 프로브 트레이스는 span 이 1개(/health/live) 또는 2개(/health/ready 는 Redis ping 포함)다.
// kubelet 이 파드마다 주기적으로 호출한다(liveness 10초 · readiness 5초).
// 프로브 상태는 kube_pod_status_ready 와 재시작 횟수로 확인한다.
var neverSampled = map[string]struct{}{
	"/health/live":  {},
	"/health/ready": {},
}

// pollingSpans = 낮은 비율로만 남길 루트 span 이름.
// otelgin 은 라우트 패턴을 span 이름으로 쓴다.
// 여기 없는 경로는 전부 남는다 — 새 경로가 조용히 표본에서 빠지는 쪽보다,
// 늘어난 양이 눈에 띄어 여기 추가하게 되는 쪽이 안전하다.
var pollingSpans = map[string]struct{}{
	"/api/admission/position": {}, // 대기 화면이 1-5초마다. 요청 수의 대부분
	"/api/admission/stats":    {}, // 메인 화면 현황 타일, 3초 고정
	"/api/admission/events":   {}, // 실황 피드, 3초 고정
}

// byRoute = 루트 span 이름으로 샘플러를 가르는 샘플러.
// 폴링만 비율로 깎고 나머지(enter·complete·leave·Kafka 발행/소비)는 전부 남긴다.
type byRoute struct {
	polling sdktrace.Sampler
	rest    sdktrace.Sampler
	never   sdktrace.Sampler
}

func (s byRoute) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	route := routeOf(p.Name)
	if _, ok := neverSampled[route]; ok {
		return s.never.ShouldSample(p)
	}
	if _, ok := pollingSpans[route]; ok {
		return s.polling.ShouldSample(p)
	}
	// ParentBased 가 감싸고 있어 이 함수는 루트 span 에만 불린다.
	// 루트인데 Client 라는 건 "밖으로 나가는 호출인데 그 위에 아무 맥락이 없다" 는 뜻이다 —
	// 배경 루프가 일 없이 도는 조회(승격 중단 플래그 · 영화 목록 · 게이지 갱신)가 여기 해당한다.
	// 승격처럼 실제로 일이 일어나는 자리는 위에서 span 을 열어 두므로 루트가 아니고, 안 깎인다.
	// 이 갈래가 없으면 유휴에도 배경 조회가 초당 열 개 넘는 트레이스를 만든다(실측 11.37 span/초).
	if p.Kind == trace.SpanKindClient {
		return s.polling.ShouldSample(p)
	}
	return s.rest.ShouldSample(p)
}

// routeOf = span 이름에서 경로만 뽑는다.
// otelgin 은 "GET /api/admission/position" 처럼 메서드를 앞에 붙인다. 메서드로 나눌 일이
// 없으므로(같은 경로면 표본 정책도 같다) 마지막 칸만 본다. 공백이 없는 이름(라우트가
// 안 잡힌 404 의 "GET", Kafka 의 "publish admissions")은 그대로 돌려주고 목록에 없어 전부 남는다.
func routeOf(spanName string) string {
	if i := strings.LastIndex(spanName, " "); i >= 0 {
		return spanName[i+1:]
	}
	return spanName
}

func (s byRoute) Description() string {
	return "ByRoute{polling=" + s.polling.Description() + ",rest=" + s.rest.Description() +
		",never=" + s.never.Description() + "}"
}

// initTracer = OTel TracerProvider 초기화. OTLP/gRPC로 span을 OTLP_GRPC_ENDPOINT
// (기본 localhost:4317)로 내보내고, W3C TraceContext propagator를 전역 등록한다 — booking·
// Kafka와 같은 전파 포맷이라야 서비스 경계를 넘어 하나의 trace로 이어진다.
// env명은 OTLP_GRPC_ENDPOINT — booking(OTLP/HTTP, 4318/v1/traces, OTLP_HTTP_ENDPOINT)과
// 프로토콜·포트가 달라 env를 분리한다(공유 시 한쪽이 깨짐).
// exporter는 lazy 연결(비블로킹)이라 Tempo가 늦게 떠도 기동을 막지 않는다.
// 반환값 shutdown은 graceful 종료 때 버퍼 span flush + exporter 종료에 쓴다.
func initTracer(ctx context.Context) (func(context.Context) error, error) {
	endpoint := os.Getenv("OTLP_GRPC_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:4317"
	}
	// otlptracegrpc.WithEndpoint는 host:port만 받는다 — booking과 맞춘 http:// 접두는 제거.
	endpoint = strings.TrimPrefix(endpoint, "http://")
	endpoint = strings.TrimPrefix(endpoint, "https://")

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(), // 온프렘 내부망 — 평문 gRPC
	)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx, resource.WithAttributes(
		attribute.String("service.name", "queue-go"),
	))
	if err != nil {
		return nil, err
	}

	// OTEL_TRACES_SAMPLER_ARG = **폴링 경로에만** 걸리는 비율(기본 0.01). 나머지는 전부 남긴다.
	//
	// 비율 하나를 전 경로에 걸면 두 요구가 충돌한다. 폴링은 사용자당 수십-수백 건이라
	//   전부 남기면 Tempo 가 버리고 Alloy RSS 가 limit 의 94% 까지 오르지만(1.0 실측),
	//   비율을 낮추면 승격→admissions→booking 체인이 100 건 중 1 건만 남아 서비스 경계를
	//   넘는 트레이스를 못 본다. 요청 종류별로 건수가 자릿수로 달라서 생기는 문제다.
	// 그래서 폴링만 낮추고 나머지는 전부 남긴다. enter 는 사용자당 1 회, 승격 발행은
	//   초당 십여 건이라 전부 남겨도 총량이 폴링에 묻힌다.
	//
	// 0.05 로 올려 본 판(2026-08-21)에서 얻은 것이 없어 0.01 로 되돌렸다.
	//   두 판의 부하 조건은 6000 VU · 6분으로 같다.
	//   exemplar 수   position 302개(0.01) → 264개(0.05). 표본을 5배로 올려도 늘지 않는다.
	//                 exemplar 는 스크레이프(15초)마다 히스토그램 버킷당 하나가 한도라
	//                 표본이 아니라 그 한도가 개수를 정한다.
	//   live traces   오픈 직후 1분에 1,150(0.01) → 10,000(0.05).
	//                 10,000 은 Tempo 의 max_traces_per_user 값이고, 그 1분에 span 4,527건이
	//                 live_traces_exceeded 로 버려졌다.
	//   평상 구간      둘 다 700-1,600 으로 한도의 7-16%. 차이가 없다.
	// 느린 요청을 훑는 것은 exemplar 가 아니라 TraceQL 검색이 한다.
	//   { name = "GET /api/admission/position" && duration > 1s }
	// 운영값은 env 로 정한다. 이 상수는 env 미설정 시의 기본값이다.
	ratio := 0.01
	if v := os.Getenv("OTEL_TRACES_SAMPLER_ARG"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 && f <= 1 {
			ratio = f
		}
	}
	// ParentBased 로 감싸면 안쪽 샘플러는 루트 span 에만 물어본다 — 자식(Redis 명령·Kafka
	//   발행)은 부모 판단을 그대로 따라, 한 트레이스가 중간에 끊기지 않는다.
	sampler := sdktrace.ParentBased(byRoute{
		polling: sdktrace.TraceIDRatioBased(ratio),
		rest:    sdktrace.AlwaysSample(),
		never:   sdktrace.NeverSample(),
	})

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, // W3C traceparent — Kafka 헤더/HTTP 전파의 규약
		propagation.Baggage{},
	))
	return tp.Shutdown, nil
}
