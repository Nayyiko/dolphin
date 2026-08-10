#!/usr/bin/env bash
# ============================================================
# 故障注入测试脚本
# 验证 Worker 宕机后任务能否被重新调度到其他 Worker。
#
# 步骤:
#   1. 创建 3 个每 5 秒执行的任务
#   2. 确认它们被调度到 Worker A
#   3. kill Worker A
#   4. 记录故障恢复时间（从 kill 到任务在 Worker B 上重新执行）
#   5. 输出恢复时间 (对标 SLO: 故障转移恢复 P99 < 30s)
#
# 用法: ./hack/test_failover.sh [WORKER_NAMES...]
# 默认: worker1 worker2 worker3 (docker-compose 中运行多实例)
# ============================================================

set -euo pipefail

SCHED_ADDR="${SCHED_ADDR:-localhost:50051}"
DOLPHINCTL="$(dirname "$0")/../bin/dolphinctl"
RESULTS_DIR="$(dirname "$0")/../results"
mkdir -p "$RESULTS_DIR"

echo "═══════════════════════════════════════════════════"
echo "  故障注入测试 — Worker 宕机后任务重分配"
echo "═══════════════════════════════════════════════════"

# 构建 dolphinctl
if [ ! -x "$DOLPHINCTL" ]; then
  echo "▶ 构建 dolphinctl..."
  (cd "$(dirname "$0")/.." && go build -o bin/dolphinctl ./cmd/dolphinctl)
fi

# 1. 创建测试任务（指向一个不存在的端点，让执行快速失败但调度链路完整）
echo ""
echo "▶ 创建 3 个每 5 秒执行的任务..."
BATCH_ID="$(date +%s)"
for i in 1 2 3; do
  $DOLPHINCTL --addr "$SCHED_ADDR" task create \
    --name "failover-test-${BATCH_ID}-${i}" \
    --cron "*/5 * * * *" \
    --handler "http://localhost:9999/failover-test" \
    --type http \
    --timeout 3 \
    --retries 1 >/dev/null 2>&1
done
echo "已创建 3 个任务 (batch=$BATCH_ID)"

# 2. 等待调度发生，找出正在执行任务的 Worker
echo ""
echo "▶ 等待首轮调度 (15s)..."
sleep 15

echo ""
echo "▶ 查询执行日志，识别活跃 Worker..."
TASKS=$($DOLPHINCTL --addr "$SCHED_ADDR" task list --limit 5 2>&1 | grep "failover-test" | grep -oP 'id=\K[^\s]+' || true)
ACTIVE_WORKERS=""

for TID in $TASKS; do
  WORKER=$($DOLPHINCTL --addr "$SCHED_ADDR" task logs --id "$TID" --limit 5 2>&1 | grep -v INSTANCE | head -3 | awk '{print $3}' | sort -u || true)
  if [ -n "$WORKER" ]; then
    ACTIVE_WORKERS="$ACTIVE_WORKERS $WORKER"
  fi
done

ACTIVE_WORKERS=$(echo $ACTIVE_WORKERS | tr ' ' '\n' | sort -u | grep -v '^$')
if [ -z "$ACTIVE_WORKERS" ]; then
  echo "⚠️  未识别到活跃 Worker。请确认 Worker 已连接并已调度任务。"
  echo "   检查: $DOLPHINCTL --addr $SCHED_ADDR task list"
  exit 1
fi

echo "活跃 Worker: $ACTIVE_WORKERS"

# 3. 选一个 Worker 杀掉
KILL_WORKER=$(echo $ACTIVE_WORKERS | awk '{print $1}')
echo ""
echo "▶ 将要 kill 的 Worker: $KILL_WORKER"
echo "  实际 kill 操作需要你手动执行 (docker kill <container> 或 kill <pid>)。"
echo "  脚本会在 kill 后检测恢复。"
echo ""
echo "  ⏳ 请在 30 秒内执行: docker kill <worker-container> 或 kill -9 <worker-pid>"
echo "  （默认假设你直接 kill 了名为 $KILL_WORKER 的进程）"
read -p "  按回车确认已 kill Worker，或输入实际 Worker 容器名继续..." CONFIRM

if [ -n "$CONFIRM" ]; then
  KILL_WORKER="$CONFIRM"
fi

# 4. 检测恢复时间
echo ""
echo "▶ 开始检测故障恢复 (最多 60s)..."
FAIL_TIME=$(date +%s)
RECOVERED=0

for i in $(seq 1 12); do
  sleep 5
  # 检查是否有新 Worker 承担了任务
  NEW_WORKERS=""
  for TID in $TASKS; do
    W=$($DOLPHINCTL --addr "$SCHED_ADDR" task logs --id "$TID" --limit 5 2>&1 | grep -v INSTANCE | awk '{print $3}' | sort -u | tr '\n' ' ' || true)
    NEW_WORKERS="$NEW_WORKERS $W"
  done

  # 如果出现了不是被 kill 的 Worker，说明已恢复
  RECOVERY_WORKERS=$(echo $NEW_WORKERS | tr ' ' '\n' | sort -u | grep -v '^$' | grep -v "$KILL_WORKER" || true)
  if [ -n "$RECOVERY_WORKERS" ]; then
    RECOVERY_TIME=$(( $(date +%s) - FAIL_TIME ))
    echo ""
    echo "✅ 检测到故障恢复！"
    echo "  被 kill 的 Worker: $KILL_WORKER"
    echo "  接管任务的 Worker: $RECOVERY_WORKERS"
    echo "  恢复时间: ${RECOVERY_TIME}s"
    echo ""
    echo "  SLO 对比: 故障转移恢复 P99 < 30s"
    if [ "$RECOVERY_TIME" -le 30 ]; then
      echo "  ✅ 达标 (${RECOVERY_TIME}s <= 30s)"
    else
      echo "  ❌ 未达标 (${RECOVERY_TIME}s > 30s)"
    fi
    RECOVERED=1
    echo "${RECOVERY_TIME}" > "$RESULTS_DIR/failover_recovery_${BATCH_ID}.txt"
    break
  fi
done

if [ "$RECOVERED" = "0" ]; then
  echo ""
  echo "❌ 60s 内未检测到恢复。"
  echo "   检查: Worker 是否配置正确、心跳超时设置 (scheduler.yaml failover.heartbeat_timeout)"
fi

echo ""
echo "═══════════════════════════════════════════════════"
echo "  故障注入测试完成。结果: results/failover_recovery_*.txt"
echo "═══════════════════════════════════════════════════"
