package main

import (
	"context"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel/trace"
)

// traceHandler = slog.Handler 데코레이터. 로그 레코드에 ctx의 OTel span에서
// trace_id·span_id를 실어 로그↔트레이스를 상관(Loki↔Tempo)시킨다.
// 표준 log 패키지(log.Printf) 경로는 ctx(=span)가 없어 두 필드가 빠진다 — 정상.
// (기존 로그 호출부 유지 원칙: 호출부는 그대로 두고 출력만 JSON으로 바꾼다.)
type traceHandler struct{ slog.Handler }

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, r)
}

func (h traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return traceHandler{h.Handler.WithAttrs(attrs)}
}

func (h traceHandler) WithGroup(name string) slog.Handler {
	return traceHandler{h.Handler.WithGroup(name)}
}

// setupLogging = slog JSON 핸들러를 기본 로거로 세운다. slog.SetDefault는 표준 log
// 패키지의 출력까지 이 핸들러로 재라우팅한다 → 기존 log.Printf 호출부가 손대지 않아도
// 그대로 JSON 한 줄로 나간다(§7 구조화 로그 규약).
func setupLogging() {
	base := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	slog.SetDefault(slog.New(traceHandler{Handler: base}))
}
