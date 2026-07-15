package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	_ "go.uber.org/automaxprocs" // GOMAXPROCS를 cgroup CPU limit에 맞춤(설계서 1부 §1, 필수 3). import만으로 동작.

	"cgv-onprem/queue-go/config"
	"cgv-onprem/queue-go/handler"
	"cgv-onprem/queue-go/kafka"
	"cgv-onprem/queue-go/metrics"
	"cgv-onprem/queue-go/processor"
	"cgv-onprem/queue-go/redis"
)

func main() {
	// 0) 구조화 로그(§7) — slog JSON을 기본 로거로. 표준 log 패키지 출력도 이리로 재라우팅되어
	//    기존 log.Printf 호출부가 그대로 JSON으로 나간다. 다른 초기화보다 먼저 세운다.
	setupLogging()

	// 1) 설정 로드(env). 없으면 로컬 기본값.
	cfg := config.Load()

	// gin 기본 debug 모드는 텍스트 배너·요청 로그를 stdout에 찍어 slog JSON 규약(§7)을
	// 오염시킨다 → 기본 release, GIN_MODE env로 재정의 가능(판정 ⑥).
	if os.Getenv("GIN_MODE") == "" {
		gin.SetMode(gin.ReleaseMode)
	}

	// 2) 배경 작업 전체를 한 ctx로 묶는다 — SIGTERM 시 cancel = graceful ③(루프·consumer 정지) 신호.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 2-1) OTel trace(§7) 초기화. exporter는 비블로킹 연결이라 Tempo가 늦어도 기동을 막지 않는다.
	//      실패해도 트레이스 없이 계속 진행 (§2-B).
	shutdownTracer, err := initTracer(ctx)
	if err != nil {
		slog.WarnContext(ctx, "otel tracer 초기화 실패(트레이스 없이 계속)", "err", err)
		shutdownTracer = func(context.Context) error { return nil }
	}

	// 3) Redis 클라이언트(lazy 연결·풀 크기는 env, 호출 타임아웃 500ms — §3-C).
	rdb := redis.New(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisPoolSize)

	// 4) Kafka — 즉시 반환(토픽 보장은 백그라운드, §2-B). Kafka가 기동을 막지 않는다.
	kp := kafka.NewProducer(ctx, cfg.KafkaBroker)

	// 5) 승격 처리율(rate) 추적 — 승격 루프가 관측, position 핸들러가 ETA 계산에 사용.
	rate := metrics.NewRateTracker()

	// 5-1) 계측(설계서 §7-B 1차 7벌) — 실패 카운터는 kafka atomic을 단일 진실원으로 노출.
	metrics.RegisterFailureCounters(
		func() float64 { return float64(kafka.PublishFailures.Load()) },
		func() float64 { return float64(kafka.ConsumeFailures.Load()) },
	)

	// 6) 배경 고루틴 4개 — runGuarded로 panic 격리·재시작(§3-D ② 채택).
	//    gin.Recovery()는 HTTP 핸들러만 지키고 이쪽은 못 지키므로 별도 래퍼가 필요.
	var wg sync.WaitGroup
	bg := func(name string, fn func(context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runGuarded(ctx, name, fn)
		}()
	}
	bg("kafka-consumer", func(c context.Context) { kafka.ConsumeCompleted(c, cfg.KafkaBroker, rdb) })
	bg("queue-processor", processor.NewQueueProcessor(rdb, cfg.MaxSessions, cfg.QueueInterval, cfg.BatchSize, kp, rate).Start)
	bg("session-timeout", processor.NewSessionTimeoutProcessor(rdb, cfg.SessionTimeout, cfg.TimeoutInterval).Start)
	bg("waiting-timeout", processor.NewWaitingTimeoutProcessor(rdb, cfg.WaitingTimeout, cfg.WaitingInterval).Start)
	// 상태 게이지 샘플러(waiting·active·rate·풀 — §7-B 행2·행4). 5s = 폴링 최대 주기와 같은 급.
	bg("metrics-sampler", func(c context.Context) { metrics.StartSampler(c, rdb, rate, 5*time.Second) })

	// 7) gin.New() = 미들웨어 0(직접 제어). 순서 중요: 계측이 Recovery보다 바깥이어야
	//    panic 요청도 히스토그램에 500으로 기록된다(Recovery가 안쪽에서 panic 흡수 →
	//    바깥 계측 Observe). 반대면 실패 요청이 계측에서 누락된다.
	r := gin.New()
	r.Use(metrics.GinMiddleware())
	r.Use(otelgin.Middleware("queue-go")) // §7 trace: 요청마다 서버 span 생성(계측은 바깥, Recovery는 안쪽 유지)
	r.Use(gin.Recovery())

	// 8) 라우트 — 헬스 + 대기열(enter·position·leave·complete).
	health := handler.NewHealth(rdb)
	health.Register(r)
	handler.NewAdmission(rdb, cfg.MaxSessions, kp, rate).Register(r)

	// 9) HTTP 서버 — r.Run() 대신 http.Server: graceful drain(Shutdown)을 쓰기 위한 교체(§2-C ②)
	//    + gin 기본은 타임아웃 무제한이라 명시(판정 ⑤ — 폴링=짧은 요청 전제를 서버가 강제).
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           r,
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second, // 폴링 keep-alive 재사용 허용
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.ErrorContext(context.Background(), "listen 실패", "err", err); os.Exit(1)
		}
	}()

	// 9-1) /metrics 전용 서버 — 앱 포트와 분리(§5-D). 인그레스·Service엔 안 물린다.
	msrv := metrics.ServeMetrics(cfg.MetricsPort)
	slog.Info("queue-go listening", "port", cfg.Port, "metricsPort", cfg.MetricsPort)

	// ===== graceful shutdown(설계서 1부 §2-C, 필수 2) =====
	// 원칙: "새 일부터 끊고, 하던 일은 끝내고, 공유 자원은 맨 마지막."
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, os.Interrupt)
	<-sig
	slog.Info("SIGTERM 수신: graceful shutdown 시작")

	// ① readiness 내림 — ready가 503으로 바뀌어 Service 명단에서 빠진다. 프로브가 볼 시간을
	//    잠깐 준다(명단 전파의 본방어는 매니페스트 preStop sleep — §2-D).
	health.BeginShutdown()
	time.Sleep(3 * time.Second)

	// ② HTTP drain — 처리 중 요청은 끝까지 응답, 새 수신은 중단. (유예 30s 안에서 상한 20s.)
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer shutCancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		// 타임아웃 반환 시 Shutdown은 미완료 커넥션을 살려둔 채 나온다 — 그대로 ④(자원 close)로
		// 가면 살아있는 핸들러가 닫힌 자원을 참조할 수 있어 강제 종료한다.
		slog.WarnContext(shutCtx, "http drain 미완 → 잔여 커넥션 강제 종료", "err", err)
		_ = srv.Close()
	}

	// ③ 루프 3종+consumer+샘플러 정지 — ctx cancel 후 전부 내려올 때까지 대기.
	cancel()
	wg.Wait()

	// ③-1 /metrics 서버는 여기서 닫는다 — ③이 걸리거나 panic 재시작이 반복되는
	// 동안에도 last-tick 창구가 살아 있어야 종료 중 진단이 가능하다.
	if err := msrv.Shutdown(shutCtx); err != nil {
		_ = msrv.Close()
	}

	// ③-2 트레이스 flush — 배경 루프·consumer가 멈춘 뒤(위 wg.Wait) 버퍼된 span을 내보내고
	//     exporter를 닫는다. 자원 close(④)보다 앞 — 종료 흔적 span까지 남긴다.
	if err := shutdownTracer(shutCtx); err != nil {
		slog.WarnContext(shutCtx, "otel tracer shutdown 실패", "err", err)
	}

	// ④ 공유 자원 close — 의존의 역순, 맨 마지막(먼저 닫으면 ②③의 마지막 작업이 죽는다).
	if err := kp.Close(); err != nil {
		slog.WarnContext(shutCtx, "kafka close 실패", "err", err)
	}
	if err := rdb.Close(); err != nil {
		slog.WarnContext(shutCtx, "redis close 실패", "err", err)
	}
	slog.Info("graceful shutdown 완료")
}

// runGuarded — 배경 루프의 panic을 프로세스 사망으로 번지지 않게 격리하고 1초 후 재시작
// (설계서 1부 §3-D ②: recover+재시작 채택). ctx 취소로 끝난 정상 종료는 그대로 반환.
func runGuarded(ctx context.Context, name string, fn func(context.Context)) {
	for {
		func() {
			defer func() {
				if r := recover(); r != nil {
					slog.ErrorContext(ctx, "panic recover", "loop", name, "panic", r, "stack", string(debug.Stack()))
				}
			}()
			fn(ctx)
		}()
		if ctx.Err() != nil {
			return
		}
		time.Sleep(time.Second)
		slog.WarnContext(ctx, "루프 재시작(panic 복구)", "loop", name)
	}
}
