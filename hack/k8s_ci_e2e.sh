#!/usr/bin/env bash
# ============================================================
# k8s_ci_e2e.sh — Dolphin 一键 K8s 端到端验证（Linux / CI）
#
# 用法（dolphin 仓库根目录）:
#   bash hack/k8s_ci_e2e.sh
#
# 流程: 起 kind → 装默认 SC(如缺) → 构建/加载镜像 → apply -k →
#       V1 网关可达 / V2 选主单主 / V3 任务真实执行(advertise_addr)
#
# 与 Windows 版 hack/k8s_kind_up.ps1 等价，供 GitHub Actions 与本地 Linux 复用。
# 跑完保留集群，方便手动复核；CI 环境用完即弃，本地可 `kind delete cluster --name dolphin` 清理。
# ============================================================
set -euo pipefail

Cluster="dolphin"
NS="dolphin"
Root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$Root"

Imgs=(
  "dolphin-gateway:latest deployments/docker/Dockerfile.gateway"
  "dolphin-scheduler:latest deployments/docker/Dockerfile.scheduler"
  "dolphin-worker:latest deployments/docker/Dockerfile.worker"
  "dolphinctl:latest deployments/docker/Dockerfile.dolphinctl"
)

pf_pids=()
cleanup() {
  for pid in "${pf_pids[@]:-}"; do kill "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT

Stage() { echo "[$(date +%H:%M:%S)] $*"; }
OK()    { echo "  [OK] $*"; }
Fail()  { echo "  [FAIL] $*"; exit 1; }
Info()  { echo "  [..] $*"; }

# ---------- 0. 前置 ----------
Stage "前置检查"
for c in docker kind kubectl go; do
  command -v "$c" >/dev/null || Fail "缺少 $c"
done
OK "docker/kind/kubectl/go 齐备"

# ---------- 1. 起 kind ----------
Stage "起 kind 集群 ($Cluster)"
kind delete cluster --name "$Cluster" 2>/dev/null || true
kind create cluster --name "$Cluster"

# ---------- 2. 默认 StorageClass（防御：kind 默认没有） ----------
# 判断标准：存在 annotation storageclass.kubernetes.io/is-default-class=true 的 SC，
# 而不是靠名字里碰 "default" 字串——名字可能叫 standard/local-path 等。
Stage "检查 StorageClass"
if kubectl get storageclass \
     -o jsonpath='{.items[*].metadata.annotations.storageclass\.kubernetes\.io/is-default-class}' \
     2>/dev/null | grep -q true; then
  OK "已有默认 StorageClass"
else
  Info "无默认 SC，安装 local-path-provisioner（hostPath）"
  kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.26/deploy/local-path-storage.yaml >/dev/null
  kubectl annotate storageclass local-path storageclass.kubernetes.io/is-default-class=true >/dev/null
  # 预拉 provisioner 镜像并加载，避免 PVC 绑定等它的运行时拉取
  lpimg="$(kubectl get deploy -n local-path-storage local-path-provisioner -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || true)"
  if [ -n "$lpimg" ]; then
    docker pull "$lpimg" >/dev/null 2>&1 || true
    kind load docker-image --name "$Cluster" "$lpimg" >/dev/null 2>&1 || true
  fi
  OK "local-path 已设为默认"
fi

# ---------- 3. 构建 + 加载 ----------
Stage "构建 4 个镜像"
for spec in "${Imgs[@]}"; do
  set -- $spec
  docker build -q -t "$1" -f "$2" .
  docker image inspect "$1" >/dev/null
  OK "$1 构建完成"
done

Stage "加载镜像进 kind 节点"
kind load docker-image --name "$Cluster" \
  dolphin-gateway:latest dolphin-scheduler:latest dolphin-worker:latest dolphinctl:latest
OK "镜像已加载"

# ---------- 3.5 预拉基础设施镜像并加载 ----------
# CI 冷环境的关键：不预拉的话，kubelet 在 pod 调度时才从 Docker Hub/quay 拉
# mysql/etcd/redis，冷拉大镜像 + 匿名拉取限流（toomanyrequests）会拖到超时
#（曾实测 mysql 360s 未 Ready）。预拉进 runner Docker + kind load，运行时零外网依赖。
# ⚠️ pull 失败的镜像跳过预加载（不要无条件 kind load——镜像不存在会让 load 直接失败）：
#    清单里 imagePullPolicy 默认 IfNotPresent，节点上没镜像时 kubelet 会运行时拉取兜底。
Stage "预拉基础设施镜像并加载进 kind"
infra_images=(mysql:8.0 quay.io/coreos/etcd:v3.5.14 redis:7-alpine)
loaded=()
for img in "${infra_images[@]}"; do
  if docker pull "$img" >/dev/null 2>&1; then
    loaded+=("$img")
  else
    Info "预拉 $img 失败（可能限流），跳过预加载，运行时由 kubelet 拉取兜底"
  fi
done
if [ "${#loaded[@]}" -gt 0 ]; then
  kind load docker-image --name "$Cluster" "${loaded[@]}"
fi
OK "基础设施镜像预加载完成（${#loaded[@]}/${#infra_images[@]}）"

# ---------- 4. 部署 ----------
Stage "部署 (kubectl apply -k deployments/k8s)"
kubectl apply -k deployments/k8s
OK "apply 完成，等待就绪（mysql 首次初始化最慢）"

Stage "等待全部 Pod Ready"
# 等待单个应用组就绪，失败时打印诊断（pod 状态 + describe + 日志），避免盲等超时
# 超时给足余量：kind 冷启动时 scheduler 的 wait-for-infra init 容器要等
# CoreDNS 就绪 + 三个依赖端口可达，曾实测 ~2m17s（见 run #20 修复记录）。
wait_ready() {
  local label="$1" timeout="$2"
  if ! kubectl -n "$NS" wait --for=condition=Ready pod -l "app=$label" --timeout="$timeout"; then
    Info "$label 未在 ${timeout} 内 Ready，打印诊断："
    kubectl -n "$NS" get pods -o wide
    kubectl -n "$NS" describe pod -l "app=$label" | tail -60
    # 容器状态精炼提取（init + 主容器），重点看 State / Last State / Exit Code / Restart Count
    kubectl -n "$NS" describe pod -l "app=$label" | grep -E -A7 '^    (Init Containers:|Containers:)' || true
    # init 容器（wait-for-infra）日志：scheduler 会逐依赖打印正在等待谁，直接定位卡点
    kubectl -n "$NS" logs -l "app=$label" -c wait-for-infra --tail=50 2>&1 || true
    kubectl -n "$NS" logs -l "app=$label" --tail=200 2>&1 || true
    Fail "$label 未就绪"
  fi
}
wait_ready mysql 600s
wait_ready etcd 120s
wait_ready redis 120s
wait_ready scheduler 300s
wait_ready worker 240s
wait_ready gateway 180s
kubectl -n "$NS" get pods
OK "全部 1/1 Ready"

# ---------- 5. V1: 网关可达 ----------
Stage "V1 · 网关可达（port-forward 8080 → /health）"
kubectl -n "$NS" port-forward svc/dolphin-gateway 8080:8080 >/dev/null 2>&1 &
pf_pids+=($!)
sleep 5
code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 http://localhost:8080/health || true)"
if [ "$code" = "200" ]; then OK "gateway /health 返回 200"; else Fail "gateway /health 返回 ${code:-N/A}"; fi

# ---------- 6. V2: 选主单主 ----------
Stage "V2 · 双 scheduler etcd 选主（应只有一个 is_leader=1）"
leader=0
for p in $(kubectl -n "$NS" get pod -l app=scheduler -o jsonpath='{.items[*].metadata.name}'); do
  v="$(kubectl -n "$NS" exec "$p" -- sh -c 'curl -s http://localhost:9090/metrics' 2>/dev/null \
       | grep '^dolphin_scheduler_is_leader ' | awk '{print $NF}' || true)"
  Info "$p is_leader=$v"
  if [ "$v" = "1" ]; then leader=$((leader+1)); fi
done
if [ "$leader" = "1" ]; then OK "恰好 1 个 Leader（防脑裂）"; else Fail "Leader 数=$leader（应=1）"; fi

# ---------- 7. V3: 任务真实执行（advertise_addr 验证） ----------
# 方法：stress create 建 N 个独立任务再 trigger-batch 全部触发。
# ⚠️ 不能用 `stress trigger --id X --count N`：它只把同一任务 next_run_at 刷 N 次，
# informer 只见一次变更 → 等效触发 1 次执行（曾跑出 4/450 的假象）。

# 汇总所有 worker 的指定指标（数据行，非 # 注释行）之和
worker_metric() {
  local pattern="$1" total=0 p val
  for p in $(kubectl -n "$NS" get pod -l app=worker -o jsonpath='{.items[*].metadata.name}'); do
    while read -r line; do
      val="${line##* }"
      [[ "$val" =~ ^[0-9]+$ ]] && total=$((total + val))
    done < <(kubectl -n "$NS" exec "$p" -- sh -c 'curl -s http://localhost:9091/metrics' 2>/dev/null \
               | grep "^$pattern" || true)
  done
  echo "$total"
}

Stage "V3 · 建 10 个任务并全部触发（worker 经 etcd 找到真实 Leader 并执行）"
# 编译到临时目录，避免在仓库根目录残留 bin/ 污染工作区（即使 .gitignore 覆盖了 bin/）
ctl_bin="$(mktemp -d)/dolphinctl"
go build -o "$ctl_bin" ./cmd/dolphinctl
kubectl -n "$NS" port-forward svc/dolphin-scheduler 50051:50051 >/dev/null 2>&1 &
pf_pids+=($!)
sleep 5

ctl() { "$ctl_bin" --addr localhost:50051 "$@"; }

ctl stress create --count 10 --prefix ci-e2e --cron "0 0 1 1 *" \
  --handler "http://dolphin-scheduler:9090/debug/sleep?seconds=1" \
  --timeout 30 --retries 3 >/dev/null
sleep 1
ids="$(ctl task list --limit 500 | grep 'name=ci-e2e' | sed -n 's/^id=\([^ ]*\).*/\1/p' | paste -sd, -)"
if [ -z "$ids" ]; then Fail "拿不到 ci-e2e 任务 id"; fi
Info "已建 $(echo "$ids" | tr ',' '\n' | wc -l | tr -d ' ') 个任务，全部触发"
ctl task trigger-batch --ids "$ids" >/dev/null

base="$(worker_metric dolphin_worker_task_completed_total)"
Info "等待 10 个完成（最多 90s）…"
deadline=$(( $(date +%s) + 90 ))
while :; do
  now="$(worker_metric dolphin_worker_task_completed_total)"
  if [ $((now - base)) -ge 10 ]; then break; fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    Fail "10 个任务未在 90s 内完成（Δ=$((now-base))）"
  fi
  sleep 5
done
OK "10/10 完成，worker 经 etcd 找到真实 Leader 并执行（advertise_addr 生效）"

# ---------- 8. 汇总 ----------
Stage "完成"
kubectl -n "$NS" get pods
cat <<'EOF'

  ✅ Dolphin 已在 kind 集群上跑通
  V1 网关可达      -> 已确认
  V2 选主单主      -> 已确认（1 个 Leader）
  V3 任务真实执行  -> 已确认（advertise_addr 生效）

  面试口径:
  "CI 里起 kind 真实集群，构建并加载镜像后部署 Dolphin——gateway 可达、
   双 scheduler 单主、worker 经 etcd 发现真实 Leader 并执行任务。"
EOF
