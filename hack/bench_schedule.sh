#!/usr/bin/env bash
# ============================================================
# 调度压测脚本
# 测量 Dolphin 的调度吞吐与延迟。
#
# 步骤:
#   1. 批量创建 N 个每 1 分钟执行一次的 HTTP 任务
#   2. 等它们到期（最多 70s）
#   3. 查询执行日志，统计调度了多少、成功率
#   4. 从指标接口拉取调度延迟分布
#
# 用法: ./hack/bench_schedule.sh [COUNT]
# 默认: COUNT=100
# ============================================================

set -euo pipefail

COUNT="${1:-100}"
SCHED_ADDR="${SCHED_ADDR:-localhost:50051}"
METRICS_URL="${METRICS_URL:-http://localhost:9090/metrics}"
PREFIX="schedbench"
RESULTS_DIR="$(dirname "$0")/../results"
mkdir -p "$RESULTS_DIR"

DOLPHINCTL="$(dirname "$0")/../bin/dolphinctl"

echo "═══════════════════════════════════════════════════"
echo "  调度压测  任务数=$COUNT  调度器=$SCHED_ADDR"
echo "═══════════════════════════════════════════════════"

# 1. 构建 dolphinctl
if [ ! -x "$DOLPHINCTL" ]; then
  echo "▶ 构建 dolphinctl..."
  (cd "$(dirname "$0")/.." && go build -o bin/dolphinctl ./cmd/dolphinctl)
fi

# 2. 批量创建任务
echo ""
echo "▶ 批量创建 $COUNT 个每 1 分钟执行的任务..."
BATCH_ID="$(date +%s)"
CREATE_START=$(date +%s%N)
$DOLPHINCTL --addr "$SCHED_ADDR" stress create \
  --count "$COUNT" \
  --prefix "${PREFIX}-${BATCH_ID}" \
  --cron "*/1 * * * *" \
  --handler "$METRICS_URL" \
  --type http \
  --timeout 5 2>&1 | tail -3
CREATE_END=$(date +%s%N)
CREATE_MS=$(( (CREATE_END - CREATE_START) / 1000000 ))
echo "创建耗时: ${CREATE_MS}ms"

# 3. 创建任务数（用于对比）
CREATED=$COUNT

# 4. 等待任务到期并执行
echo ""
echo "▶ 等待任务到期执行 (最多 70s)..."
WAITED=0
while [ $WAITED -lt 70 ]; do
  # 检查调度指标: 是否有 dispatch 发生
  if curl -sf "$METRICS_URL" 2>/dev/null | grep -q "dolphin_scheduler_dispatch_total" ; then
    break
  fi
  sleep 5
  WAITED=$((WAITED + 5))
done

# 5. 再等 20s 让任务跑完上报
echo "▶ 等待执行完成 (20s)..."
sleep 20

# 6. 拉取指标
echo ""
echo "▶ 拉取调度指标..."
if curl -sf "$METRICS_URL" > "$RESULTS_DIR/schedule_metrics_${BATCH_ID}.txt" 2>/dev/null; then
  echo "--- 调度分发计数 (总) ---"
  grep "dolphin_scheduler_dispatch_total" "$RESULTS_DIR/schedule_metrics_${BATCH_ID}.txt" | head -3 || echo "  (无)"
  echo ""
  echo "--- 调度延迟直方图 ---"
  grep "dolphin_scheduler_task_lag_seconds_bucket" "$RESULTS_DIR/schedule_metrics_${BATCH_ID}.txt" | head -10 || echo "  (无)"
else
  echo "⚠️  无法访问 $METRICS_URL（scheduler 的 metrics 端口）"
fi

# 7. 用日志查询验证
echo ""
echo "▶ 查询最近任务的执行日志..."
$DOLPHINCTL --addr "$SCHED_ADDR" task list --limit 3 2>&1 | head -5

echo ""
echo "═══════════════════════════════════════════════════"
echo "  压测完成。原始指标: results/schedule_metrics_${BATCH_ID}.txt"
echo "═══════════════════════════════════════════════════"
