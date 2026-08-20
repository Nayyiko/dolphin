# Dolphin K8s 部署实测清单（kind · Windows）

> 目标：面试前在**真实 K8s 集群**跑通 Dolphin 部署，留下硬数据。
> 面试价值：把"写了 manifest"升级成"在 kind 集群上完整跑通——gateway 30080 可达、双 scheduler 经 etcd 选主单主、worker 通过 etcd watch 自动跟随并真实执行任务、并发超过容量时背压真实触发、最终全部成功"。
> 预计耗时：20–40 分钟（首次拉镜像另算）。所有命令在 **PowerShell** 里从 **dolphin 仓库根目录**执行。

---

## 前置条件（一次性，未装则先装）

```powershell
# 1. Docker Desktop 已启动（WSL2 backend），docker info 能通
docker info

# 2. kind
winget install Kind

# 3. kubectl
winget install Kubernetes.kubectl

# 4. 验证
kind version
kubectl version --client
```

---

## 第 0 步 · 起集群 + 构建并加载镜像

```powershell
# 起 kind 集群（单节点足够演示）
kind create cluster --name dolphin

# 构建四个镜像（根目录执行）
docker build -t dolphin-gateway:latest   -f deployments/docker/Dockerfile.gateway   .
docker build -t dolphin-scheduler:latest -f deployments/docker/Dockerfile.scheduler .
docker build -t dolphin-worker:latest    -f deployments/docker/Dockerfile.worker    .
docker build -t dolphinctl:latest        -f deployments/docker/Dockerfile.dolphinctl .

# 加载进 kind 节点（⚠️ 关键，不加载会 ImagePullFailed）
kind load docker-image --name dolphin dolphin-gateway:latest dolphin-scheduler:latest dolphin-worker:latest dolphinctl:latest
```

---

## 第 1 步 · 部署并等待就绪

```powershell
kubectl apply -k deployments/k8s
kubectl -n dolphin get pods -w
```

预期（1–2 分钟内全部 Running）：
`etcd-0` 1/1 · `mysql` 1/1 · `redis` 1/1 · `scheduler`×2 · `worker`×2 · `gateway`×2

```powershell
kubectl -n dolphin get pods -o wide
kubectl -n dolphin get svc
```

---

## 第 2 步 · 五项验证（逐项留输出）

### V1 · Gateway 可达（NodePort 30080）

```powershell
# 方案 A：port-forward（Windows 上最稳）
kubectl -n dolphin port-forward svc/dolphin-gateway 8080:8080
# 另开一个终端
curl http://localhost:8080/health
```

预期：返回 `200 OK` 类健康响应。

面试话术：gateway 2 副本经 Service 暴露，NodePort 30080 供集群外访问，`/health` 探活通过；限流/熔断在网关层，见 V6。

---

### V2 · 双 scheduler etcd 选主（单主验证，防脑裂）

```powershell
kubectl -n dolphin logs deploy/scheduler | Select-String "became leader"
```

预期：选主事件只来自**一个**副本（对比 pod 名）。再确认指标：

```powershell
kubectl -n dolphin exec deploy/scheduler -- sh -c 'curl -s http://localhost:9090/metrics' | Select-String "is_leader"
```

预期：`dolphin_scheduler_is_leader{...} 1` 只出现一次（另一副本为 0）。

面试话术：etcd Lease 选主，Lease TTL 15s、每 5s 续约；Lease 过期后旧主写入被 etcd 拒绝——机制上杜绝双主（对比 Redis 锁）。

---

### V3 · Worker 自动跟随 + 任务真实执行（advertise_addr 在集群内验证）

> 这是整个部署的**核心验证**。advertise_addr 若没生效，worker 会连自己 Pod 的 localhost，任务全挂。

```powershell
# 集群内跑 dolphinctl：建 100 个每分钟执行的任务，handler 打 gateway 健康端点
kubectl -n dolphin run ctl --rm -it --image=dolphinctl:latest --image-pull-policy=IfNotPresent --restart=Never -- `
  --addr dolphin-scheduler:50051 stress create --count 100 --prefix kind --cron "*/1 * * * *" --handler http://dolphin-gateway:8080/health

# 等 70–90 秒（任务已跑到下一分钟），看 worker 完成计数
$p = (kubectl -n dolphin get pod -l app=worker -o jsonpath='{.items[0].metadata.name}')
kubectl -n dolphin exec $p -- sh -c 'curl -s http://localhost:9091/metrics' | Select-String "dolphin_worker_task_completed_total|dolphin_worker_pool_inflight|dolphin_worker_tasks_executing"
```

预期：`dolphin_worker_task_completed_total` 持续增长（几分钟内累计到数百）；任务真实被两个 worker 执行、结果上报成功。

面试话术：这是 advertise_addr 排障故事的**实证**——单机 docker-compose 连 localhost 没问题，上集群后我用 Downward API 把 `$(POD_IP):50051` 注入为发布地址，worker 从 etcd watch 到真实 leader 地址并成功执行任务；本地环境掩盖的分布式问题在集群里被验证修复了。

---

### V4 · kill 一个 worker 副本（心跳重分发）

```powershell
$p = (kubectl -n dolphin get pod -l app=worker -o jsonpath='{.items[0].metadata.name}')
kubectl -n dolphin delete pod $p
# 等 1 分钟，确认新的 worker 起来后继续执行任务（completed 计数继续增长）
kubectl -n dolphin get pods -l app=worker
$p2 = (kubectl -n dolphin get pod -l app=worker -o jsonpath='{.items[0].metadata.name}')
kubectl -n dolphin exec $p2 -- sh -c 'curl -s http://localhost:9091/metrics' | Select-String "dolphin_worker_task_completed_total"
```

预期：另一个 worker 接管 running 任务，无任务永久卡死；新副本启动后继续执行。

面试话术：心跳滑动窗口判定失联 → running 任务重分发；本地 failover_test 的 N=5 恢复分布在集群里复现。

---

### V5 · 背压真实触发（可选但推荐）

> configmap 里 worker pool `capacity: 50`，2 个 worker 共 300 在途上限。一次触发 400 个会超过容量，触发"拒绝 → 自动重试"。
>
> ⚠️ 必须用"多个独立任务"制造并发：`stress trigger --id X --count N` 只是把**同一个任务**的 `next_run_at` 刷 N 次，informer 只看到一次变更 → 等效触发 1 次执行，压测是假的。正确做法是 `stress create` 建 N 个任务 → 收集 id → `task trigger-batch` 全部触发。

```powershell
# 建 400 个独立任务（sleep 10s > 下发跨度，保证在途冲顶 300 上限前无人完成）
kubectl -n dolphin run ctl --rm -it --image=dolphinctl:latest --image-pull-policy=IfNotPresent --restart=Never -- `
  --addr dolphin-scheduler:50051 stress create --count 400 --prefix bp --cron "0 0 1 1 *" --handler "http://dolphin-scheduler:9090/debug/sleep?seconds=10" --timeout 30 --retries 3

# 收集 bp- 前缀任务 id（逐行取再 join；勿用 SQL GROUP_CONCAT，170+ 个 UUID 会被 1024 截断）
$lst = kubectl -n dolphin run ctl --rm -it --image=dolphinctl:latest --image-pull-policy=IfNotPresent --restart=Never -- `
  --addr dolphin-scheduler:50051 task list --limit 500
$ids = ($lst | Select-String 'name=bp-' | ForEach-Object { ($_ -replace '^id=(\S+).*','$1') }) -join ','

# 全部触发
kubectl -n dolphin run ctl --rm -it --image=dolphinctl:latest --image-pull-policy=IfNotPresent --restart=Never -- `
  --addr dolphin-scheduler:50051 task trigger-batch --ids $ids

# 观察：池满拒绝 + 重试 + 最终全部完成
$p = (kubectl -n dolphin get pod -l app=worker -o jsonpath='{.items[0].metadata.name}')
kubectl -n dolphin exec $p -- sh -c 'curl -s http://localhost:9091/metrics' | Select-String "dolphin_worker_pool_rejected_total|dolphin_worker_pool_capacity_utilization|dolphin_worker_task_completed_total"
kubectl -n dolphin logs deploy/scheduler | Select-String "retry|Retry|backoff" | Select-Object -Last 10
```

预期：`dolphin_worker_pool_rejected_total` 出现增量（拒绝），`dolphin_scheduler_retry_total` 增长（重试），`dolphin_worker_task_completed_total` 最终到达 400，全部成功。

面试话术：主动背压而非无限堆积——在途超容量直接拒绝、调度器自动重试，持久化期望态 + 幂等 + 背压 = 不用 MQ 也能论证"不丢不重"。本地 retry_test 的 170 任务场景在集群里复现。

---

### V6 · 限流多实例共享（可选）

```powershell
kubectl -n dolphin port-forward svc/dolphin-gateway 8080:8080
# 两个终端同时各打 300 个请求（或一个终端快速打 600）
for ($i=0; $i -lt 300; $i++) { Invoke-WebRequest -Uri http://localhost:8080/health -UseBasicParsing -TimeoutSec 2 | Out-Null }
# 看网关拒绝计数
$p = (kubectl -n dolphin get pod -l app=gateway -o jsonpath='{.items[0].metadata.name}')
kubectl -n dolphin exec $p -- sh -c 'curl -s http://localhost:8080/metrics' | Select-String "dolphin_gateway_ratelimit_rejected_total|dolphin_gateway_requests_total"
```

预期：两个 gateway 副本共享 Redis 令牌桶，拒绝数不因多副本而翻倍（对比独立桶）。

面试话术：Redis + Lua 原子令牌桶，多实例共享同一桶——N 副本总量不翻倍，本地 15000 请求 0.7% 误差的实证在集群里复现。

---

## 第 3 步 · 面试留证据（跑完把这些贴进 `99-面后复盘` 或单独存档）

| 验证 | 证据输出 |
|------|---------|
| V2 选主单主 | `is_leader 1` 只出现一次的日志/指标片段 |
| V3 执行链路 + advertise_addr | worker `completed_total` 增长截图 + 100 任务成功 |
| V4 心跳重分发 | kill worker 后 completed 计数继续增长的输出 |
| V5 背压 | `rejected_total` + `retry_total` + 最终全部成功的计数 |
| V6 共享限流 | 拒绝数不翻倍的计数对比 |

一句话总结面试口径：**"我写的 K8s 部署在 kind 集群上完整跑通：gateway 可达、双 scheduler 单主、worker 自动跟随并执行 100+ 任务、kill 副本自愈、200 并发触发背压后全部成功。"**

---

## 常见坑

| 症状 | 原因 | 处理 |
|------|------|------|
| 镜像 ImagePullFailed / ErrImagePull | 没 `kind load docker-image`，或 tag 拼错 | 重新 load，确认 manifest 里 `imagePullPolicy: IfNotPresent` |
| etcd-0 一直 Pending | PVC 冲突（上次集群残留） | `kind delete cluster --name dolphin` 重来 |
| 任务全失败，worker 日志出现连 localhost | advertise_addr 没生效（改过 scheduler.yaml 时） | 检查 `$(POD_IP):50051` 注入，重启 scheduler |
| `kubectl run ctl` 卡住 | `--rm -it` 交互；镜像没 load | 确认 dolphinctl 已 load；或改用 port-forward + 本机 `go build -o bin/dolphinctl ./cmd/dolphinctl` |
| 指标抓不到 | worker 的 metrics 在 9091，gateway 在 8080，scheduler 在 9090 | 按端口区分；port-forward 用对应端口 |
| Windows PowerShell 转义 | 多行命令用反引号 `` ` `` 续行 | 或用 git-bash；`--handler` URL 用双引号包 |

---

## 清理

```powershell
kind delete cluster --name dolphin
```
