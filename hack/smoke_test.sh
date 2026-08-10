#!/usr/bin/env bash
# ============================================================
# 端到端冒烟测试脚本
# 验证 Dolphin 完整链路: 创建任务 → 调度 → Worker 执行 → 结果记录
#
# 前置:
#   - docker-compose up (etcd/mysql/redis)
#   - scheduler 已启动 (localhost:50051)
#   - worker 已启动
#   - gateway 已启动 (localhost:8080, 可选)
#
# 用法: ./hack/smoke_test.sh
# ============================================================

set -euo pipefail

SCHED_ADDR="${SCHED_ADDR:-localhost:50051}"
GATEWAY_URL="${GATEWAY_URL:-http://localhost:8080}"
DOLPHINCTL="$(dirname "$0")/../bin/dolphinctl"
RESULTS_DIR="$(dirname "$0")/../results"
mkdir -p "$RESULTS_DIR"

echo "═══════════════════════════════════════════════════"
echo "  Dolphin 端到端冒烟测试"
echo "═══════════════════════════════════════════════════"

PASS=0
FAIL=0

check() {
  local name="$1"
  local ok="$2"
  if [ "$ok" = "true" ]; then
    echo "  ✅ $name"
    PASS=$((PASS + 1))
  else
    echo "  ❌ $name"
    FAIL=$((FAIL + 1))
  fi
}

# ── 1. 基础设施检查 ──
echo ""
echo "▶ [1] 基础设施检查"

# scheduler gRPC 端口
if nc -z localhost 50051 2>/dev/null || curl -sf --connect-timeout 3 http://localhost:50051 >/dev/null 2>&1; then
  check "scheduler gRPC 端口 50051 可达" true
else
  check "scheduler gRPC 端口 50051 可达" false
fi

# 构建 dolphinctl
if [ ! -x "$DOLPHINCTL" ]; then
  echo "▶ 构建 dolphinctl..."
  (cd "$(dirname "$0")/.." && go build -o bin/dolphinctl ./cmd/dolphinctl)
fi

# ── 2. 创建任务 ──
echo ""
echo "▶ [2] 创建任务"
BATCH_ID="$(date +%s)"
CREATE_OUT=$($DOLPHINCTL --addr "$SCHED_ADDR" task create \
  --name "smoke-${BATCH_ID}" \
  --cron "*/1 * * * *" \
  --handler "http://localhost:9090/healthz" \
  --type http \
  --timeout 5 \
  --retries 1 2>&1)

if echo "$CREATE_OUT" | grep -q "id="; then
  TASK_ID=$(echo "$CREATE_OUT" | grep -oP 'id=\K[^\s]+')
  check "创建任务成功 (id=$TASK_ID)" true
else
  check "创建任务成功" false
  echo "  输出: $CREATE_OUT"
  echo ""
  echo "失败。常见原因:"
  echo "  - scheduler 未启动"
  echo "  - MySQL 未连接 (docker-compose up mysql)"
  echo "  - etcd 未连接"
  exit 1
fi

# ── 3. 查询任务 ──
echo ""
echo "▶ [3] 查询任务"
GET_OUT=$($DOLPHINCTL --addr "$SCHED_ADDR" task get --id "$TASK_ID" 2>&1)
if echo "$GET_OUT" | grep -q "id=$TASK_ID"; then
  check "查询任务成功" true
else
  check "查询任务成功" false
fi

# ── 4. 手动触发 ──
echo ""
echo "▶ [4] 手动触发任务"
TRIGGER_OUT=$($DOLPHINCTL --addr "$SCHED_ADDR" task trigger --id "$TASK_ID" 2>&1)
if echo "$TRIGGER_OUT" | grep -q "triggered"; then
  check "手动触发成功" true
else
  check "手动触发成功" false
fi

# ── 5. 等待调度与执行 ──
echo ""
echo "▶ [5] 等待调度与执行 (30s)..."
sleep 30

LOGS_OUT=$($DOLPHINCTL --addr "$SCHED_ADDR" task logs --id "$TASK_ID" --limit 5 2>&1)
if echo "$LOGS_OUT" | grep -qE "success|failed|timeout"; then
  check "任务被执行 (有执行记录)" true
  echo "  最近执行:"
  echo "$LOGS_OUT" | head -4 | sed 's/^/    /'
else
  check "任务被执行 (有执行记录)" false
  echo "  日志输出: $LOGS_OUT"
fi

# ── 6. 网关检查 ──
echo ""
echo "▶ [6] 网关检查"
if curl -sf --connect-timeout 3 "$GATEWAY_URL/health" >/dev/null 2>&1; then
  check "网关 /health 可达" true
else
  check "网关 /health 可达" false
fi

if curl -sf --connect-timeout 3 "$GATEWAY_URL/metrics" 2>/dev/null | grep -q "dolphin_gateway"; then
  check "网关指标输出正常" true
else
  check "网关指标输出正常" false
fi

# ── 7. 调度器指标 ──
echo ""
echo "▶ [7] 调度器指标"
if curl -sf --connect-timeout 3 "http://localhost:9090/metrics" 2>/dev/null | grep -q "dolphin_scheduler"; then
  check "调度器指标输出正常" true
else
  check "调度器指标输出正常" false
fi

# ── 总结 ──
echo ""
echo "═══════════════════════════════════════════════════"
echo "  冒烟测试结果: ✅ $PASS 通过, ❌ $FAIL 失败"
echo "═══════════════════════════════════════════════════"

# 保存结果
{
  echo "Dolphin 冒烟测试 $(date)"
  echo "通过: $PASS  失败: $FAIL"
  echo "Task ID: $TASK_ID"
} > "$RESULTS_DIR/smoke_${BATCH_ID}.txt"

if [ "$FAIL" -gt 0 ]; then
  exit 1
fi
echo ""
echo "全部通过！详细结果: results/smoke_${BATCH_ID}.txt"
