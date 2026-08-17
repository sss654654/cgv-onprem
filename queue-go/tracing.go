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
)

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

	// 샘플링 비율. 기본 1.0 — 데모에서 요청 하나가 어디까지 갔는지 전부 보려는 값이고
	//   booking 의 sampling.probability=1.0 과 짝이다.
	// 부하 판에서는 내려야 한다. 사용자 1,000명 판이 약 9만 span 인데, Tempo 가 20일 동안
	//   받은 누적이 35,021 span 이다 — 판 한 번이 그 2.5배를 5분에 쏟는다.
	// 값이 1 미만이면 부모 판단을 따르는 비율 샘플러를 쓴다(같은 트레이스가 중간에
	//   끊기지 않게). 잘못된 값이면 1.0 으로 둔다.
	ratio := 1.0
	if v := os.Getenv("OTEL_TRACES_SAMPLER_ARG"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 && f <= 1 {
			ratio = f
		}
	}
	sampler := sdktrace.AlwaysSample()
	if ratio < 1.0 {
		sampler = sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
	}

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
