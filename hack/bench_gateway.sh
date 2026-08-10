#!/usr/bin/env bash
# ============================================================
# 网关压测脚本
# 用法: ./hack/bench_gateway.sh [QPS] [DURATION]
# 默认: QPS=1000, DURATION=30s
#
# 前置: gateway 已在 localhost:8080 运行, 基础设施已启动
# ============================================================

set -euo pipefail

QPS="${1:-1000}"
DURATION="${2:-30}"
GATEWAY_URL="${GATEWAY_URL:-http://localhost:8080}"
RESULTS_DIR="$(dirname "$0")/../results"

mkdir -p "$RESULTS_DIR"

echo "═══════════════════════════════════════════════════"
echo "  网关压测  URL=$GATEWAY_URL  QPS=$QPS  DURATION=${DURATION}s"
echo "═══════════════════════════════════════════════════"

# ── 检查 wrk ──
if command -v wrk >/dev/null 2>&1; then
  echo ""
  echo "▶ [1/3] /health 端点 (QPS=$QPS)"
  wrk -t4 -c100 -d${DURATION}s -R${QPS} --latency "$GATEWAY_URL/health" \
    2>&1 | tee "$RESULTS_DIR/gateway_health.txt"

  echo ""
  echo "▶ [2/3] /metrics 端点 (QPS=$QPS)"
  wrk -t4 -c100 -d${DURATION}s -R${QPS} --latency "$GATEWAY_URL/metrics" \
    2>&1 | tee "$RESULTS_DIR/gateway_metrics.txt"

  echo ""
  echo "▶ [3/3] 根路径 / (QPS=$QPS)"
  wrk -t4 -c100 -d${DURATION}s -R${QPS} --latency "$GATEWAY_URL/" \
    2>&1 | tee "$RESULTS_DIR/gateway_root.txt"

  echo ""
  echo "结果已保存到: $RESULTS_DIR/"
  exit 0
fi

# ── wrk 不可用: 检查 vegeta ──
if command -v vegeta >/dev/null 2>&1; then
  echo ""
  echo "▶ vegeta 压测 /health (QPS=$QPS, ${DURATION}s)"
  echo "GET $GATEWAY_URL/health" | vegeta attack \
    -rate=$QPS -duration=${DURATION}s -workers=50 \
    | tee "$RESULTS_DIR/gateway_health.bin" \
    | vegeta report
  echo ""
  echo "▶ vegeta 压测 /metrics (QPS=$QPS, ${DURATION}s)"
  echo "GET $GATEWAY_URL/metrics" | vegeta attack \
    -rate=$QPS -duration=${DURATION}s -workers=50 \
    | tee "$RESULTS_DIR/gateway_metrics.bin" \
    | vegeta report
  exit 0
fi

echo "⚠️  未找到 wrk 或 vegeta。"

# ── 都不可用: 用 Go 内置压测器 ──
echo "尝试安装 wrk..."
if command -v brew >/dev/null 2>&1; then
  brew install wrk
  echo "已安装 wrk, 请重新运行本脚本。"
elif command -v apt-get >/dev/null 2>&1 && [ "$(id -u)" = "0" ]; then
  apt-get install -y wrk >/dev/null 2>&1 || echo "apt 安装失败"
  echo "已安装 wrk, 请重新运行本脚本。"
else
  echo ""
  echo "手动安装 wrk:"
  echo "  macOS:  brew install wrk"
  echo "  Ubuntu: sudo apt-get install wrk"
  echo "  Windows: choco install wrk 或使用 vegeta (go install github.com/tsenart/vegeta/v12@latest)"
  echo ""
  echo "或者用已有的微基准:"
  echo "  make bench"
fi

exit 1
