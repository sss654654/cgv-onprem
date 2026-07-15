package main

import (
	"context"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// initTracer = OTel TracerProvider 초기화(§7 trace). OTLP/gRPC로 span을 OTLP_GRPC_ENDPOINT
// (기본 localhost:4317)로 내보내고, W3C TraceContext propagator를 전역 등록한다 — booking·
// Kafka와 같은 전파 포맷이라야 서비스 경계를 넘어 하나의 trace로 이어진다.
// env명은 OTLP_GRPC_ENDPOINT — booking(OTLP/HTTP, 4318/v1/traces, OTLP_HTTP_ENDPOINT)과
// 프로토콜·포트가 달라 env를 분리한다(공유 시 한쪽이 깨짐).
// exporter는 lazy 연결(비블로킹)이라 Tempo가 늦게 떠도 기동을 막지 않는다(§2-B).
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
		otlptracegrpc.WithInsecure(), // 온프렘 내부망 — 평문 gRPC(§5)
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

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()), // 데모 1.0(booking sampling.probability=1.0과 동일)
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, // W3C traceparent — Kafka 헤더/HTTP 전파의 규약
		propagation.Baggage{},
	))
	return tp.Shutdown, nil
}
