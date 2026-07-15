package config

import (
	"os"
	"strconv"
	"time"
)

// Config는 앱 설정(폴링 재설계 기준). 값은 환경변수로 주입, 없으면 기본값(로컬).
type Config struct {
	Port          string        // 리슨 포트 (기본 8090)
	RedisAddr     string        // host:port (go-redis Options.Addr)
	RedisPassword string        // 로컬은 빈 값, prod는 주입
	MaxSessions   int64         // 입장 정원(§0). 로컬 기본 2.
	QueueInterval time.Duration // ③ 승격 주기 (기본 2초)
	BatchSize     int64         // ③ 한 번에 승격 상한(안전밸브). 로컬 기본 100.
	SessionTimeout  time.Duration // §1-4a active 세션 수명. 데모 기본 60초(좌석락 45s < 세션 60s). 실운영 env로 600.
	TimeoutInterval time.Duration // §1-4a active 만료 검사 주기 (기본 10초)
	WaitingTimeout  time.Duration // §1-4b 폴링 끊긴 대기자 evict 임계(폴링 주기보다 넉넉히). 기본 30초.
	WaitingInterval time.Duration // §1-4b waiting 만료 검사 주기 (기본 10초)
	KafkaBroker     string        // 서비스간 통신(admissions·bookings-completed)
	RedisPoolSize   int           // Redis 커넥션 풀 크기. 0=라이브러리 기본(10×GOMAXPROCS — automaxprocs 교정 후 CPU limit 기준). 값은 부하 실측으로 확정(설계서 1부 §1).
	MetricsPort     string        // /metrics 전용 포트(§5-D 분리 결정). 인그레스·Service엔 안 물리고 ServiceMonitor만 안다.
}

// Load는 환경변수에서 설정을 읽는다. REDIS_HOST/REDIS_PORT를 합쳐 Addr로.
func Load() Config {
	cfg := Config{
		Port:          getenv("PORT", "8090"),
		RedisAddr:     getenv("REDIS_HOST", "localhost") + ":" + getenv("REDIS_PORT", "6379"),
		RedisPassword: getenv("REDIS_PASSWORD", ""),
		MaxSessions:     getenvInt("MAX_SESSIONS", 2),
		QueueInterval:   time.Duration(getenvInt("QUEUE_PROCESS_INTERVAL", 2000)) * time.Millisecond,
		BatchSize:       getenvInt("PROCESSING_BATCH_SIZE", 100),
		SessionTimeout:  time.Duration(getenvInt("SESSION_TIMEOUT", 60)) * time.Second,
		TimeoutInterval: time.Duration(getenvInt("SESSION_CLEANUP_INTERVAL", 10000)) * time.Millisecond,
		WaitingTimeout:  time.Duration(getenvInt("WAITING_TIMEOUT", 30)) * time.Second,
		WaitingInterval: time.Duration(getenvInt("WAITING_CLEANUP_INTERVAL", 10000)) * time.Millisecond,
		KafkaBroker:     getenv("KAFKA_BROKER", "localhost:9092"),
		RedisPoolSize:   int(getenvInt("REDIS_POOL_SIZE", 0)),
		MetricsPort:     getenv("METRICS_PORT", "9091"),
	}
	// 하한 가드 — batch·정원이 0/음수면 승격 Lua의 전제가 깨짐(promote.go 가드와 이중 방어).
	if cfg.BatchSize < 1 {
		cfg.BatchSize = 100
	}
	if cfg.MaxSessions < 1 {
		cfg.MaxSessions = 1
	}
	return cfg
}

// getenvInt는 환경변수를 정수로 읽는다(없거나 파싱 실패면 기본값).
func getenvInt(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

// getenv는 환경변수가 비어있으면 기본값을 돌려준다.
func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
