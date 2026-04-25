#!/bin/bash
# One-click runner for Chapter-5-style descent benchmark (no K8s simulator needed).
set -euo pipefail

BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info() { echo -e "${BLUE}ℹ️  $1${NC}"; }
ok() { echo -e "${GREEN}✅ $1${NC}"; }
warn() { echo -e "${YELLOW}⚠️  $1${NC}"; }
err() { echo -e "${RED}❌ $1${NC}"; }

dc() {
  if [ -f "compose.quantum.yml" ]; then
    docker-compose -f compose.yml -f compose.quantum.yml "$@"
  else
    docker-compose -f compose.yml "$@"
  fi
}

main() {
  if [ ! -f "compose.yml" ] || [ ! -f "benchmark_quantum_descent.py" ]; then
    err "请在 Grover-kube-scheduler-simulator 目录下运行此脚本"
    exit 1
  fi

  info "启动 quantum-service (docker compose)..."
  dc up -d --build quantum-service

  info "等待 quantum-service 就绪..."
  for i in {1..30}; do
    if curl -s http://localhost:8000/health >/dev/null 2>&1; then
      ok "quantum-service 已就绪"
      break
    fi
    if [ "$i" -eq 30 ]; then
      warn "quantum-service 健康检查超时，但继续尝试跑基准"
    fi
    sleep 2
  done

  info "运行 budget-descent 基准（会写入 results/quantum_descent_benchmark.csv）..."
  python benchmark_quantum_descent.py "$@"

  ok "完成"
}

main "$@"

