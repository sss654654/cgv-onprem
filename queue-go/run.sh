#!/usr/bin/env bash
# run.sh — queue-go 로컬 기동(임시). WSL에서 실행.
# 서비스가 늘면(booking·frontend) docker-compose로 대체될 자리표시.
#
# 사용: bash run.sh   또는   ./run.sh
# 환경변수로 튜닝 가능: MAX_SESSIONS=5 SESSION_TIMEOUT=5 ./run.sh
set -e

export PATH="$PATH:/usr/local/go/bin"
cd "$(dirname "$0")"

# 1) Redis(네이티브 6379) 떠있나 확인. 없으면 안내하고 종료.
if ! redis-cli ping >/dev/null 2>&1; then
  echo "[run] Redis가 안 떠있음. 먼저: sudo service redis-server start"
  exit 1
fi

# 2) 로컬 기본 튜닝값(이미 set돼 있으면 그대로 — 오버라이드 가능).
export PORT="${PORT:-8090}"                                 # 8080은 고아 docker-proxy 점유라 8090
export MAX_SESSIONS="${MAX_SESSIONS:-2}"                    # 입장 정원(임시)
export SESSION_TIMEOUT="${SESSION_TIMEOUT:-60}"            # active 세션 수명(초). 데모 60s(좌석락 45s < 세션 60s) — config.go·compose와 정합
export QUEUE_PROCESS_INTERVAL="${QUEUE_PROCESS_INTERVAL:-2000}"   # 승격 주기(ms)
export SESSION_CLEANUP_INTERVAL="${SESSION_CLEANUP_INTERVAL:-10000}" # 만료 검사(ms)

echo "[run] queue-go 기동 → http://localhost:$PORT  (Redis: $(redis-cli ping), 정원=$MAX_SESSIONS)"
go run .
