# Dolphin 验证与压测指南

本指南包含在本机完整验证 Dolphin 的步骤，以及压测方法。压测数据是面试时证明系统能力的关键证据。

---

## 一、本机验证清单（端到端）

### 1. 启动基础设施

```bash
cd C:\Users\30641\Desktop\dolphin
docker-compose -f deployments/docker-compose.yaml up -d etcd mysql redis
```

检查：`docker ps` 应看到 `dolphin-etcd`、`dolphin-mysql`、`dolphin-redis` 三个容器 running。

### 2. 启动三个组件（三个终端）

```bash
# 终端 1 — 调度器
make run-scheduler

# 终端 2 — Worker
make run-worker

# 终端 3 — 网关
make run-gateway
```

**预期的启动日志：**
- scheduler: `informer: initial list synced count=0` → `became leader`
- worker: `registered with scheduler accepted=true`
- gateway: `gateway listening port=8080`

### 3. 端到端冒烟测试

```bash
make smoke
```

预期输出全部 ✅。如果某步失败，见文末"常见问题"。

### 4. 手动验证

```bash
# 创建任务
./bin/dolphinctl task create \
  --name "演示任务" \
  --cron "*/1 * * * *" \
  --handler "http://localhost:9090/healthz" \
  --type http

# 列出任务（记录输出的 id）
./bin/dolphinctl task list

# 手动触发（立即执行一次）
./bin/dolphinctl task trigger --id <TASK_ID>

# 等 10 秒后查执行日志
./bin/dolphinctl task logs --id <TASK_ID>
```

**预期**：logs 中出现 `success` 状态、`worker_id` 非空。

### 5. 查看指标

```bash
curl localhost:9090/metrics | grep dolphin_scheduler
curl localhost:9091/metrics | grep dolphin_worker
curl localhost:8080/metrics | grep dolphin_gateway
```

---

## 二、压测方法

### A. 网关压测（QPS / 延迟）

需要安装 [wrk](https://github.com/wg/wrk) 或 [vegeta](https://github.com/tsenart/vegeta)。

```bash
# wrk: 1000 QPS 打 30 秒
make bench-gateway QPS=1000 DURATION=30

# 或指定参数
./hack/bench_gateway.sh 2000 30
```

**预期指标（面试用）：**

| 指标 | 目标值 | 实际值（待填） |
|------|--------|----------------|
| 网关 QPS | 10,000+ | ____ |
| P99 延迟 | < 5ms | ____ |
| P95 延迟 | < 2ms | ____ |
| 错误率 | < 0.1% | ____ |

### B. 调度压测（调度吞吐 / 延迟）

```bash
# 创建 100 个每 1 分钟执行的任务
make bench-schedule COUNT=100
```

脚本会批量创建任务 → 等待执行 → 拉取调度延迟直方图。

**预期指标（面试用）：**

| 指标 | 目标值 | 实际值（待填） |
|------|--------|----------------|
| 任务创建速率 | 100+ tasks/sec | ____ |
| 调度延迟 P99 | < 2s | ____ |
| 调度成功率 | > 99.9% | ____ |

### C. 故障注入（故障转移恢复时间）

```bash
make failover
```

脚本会创建任务 → 识别活跃 Worker → 让你 kill 掉一个 → 检测恢复时间。

**预期指标（面试用，对标 SLO）：**

| 指标 | SLO | 实际值（待填） |
|------|-----|----------------|
| 故障转移恢复时间 | < 30s | ____ |

### D. 完整闭环压测（一键）

无需安装 wrk/vegeta——内置 `loadgen`（Go 实现）。Windows 下运行：

```powershell
# 1. 构建压测工具
go build -o bin\dolphinctl.exe ./cmd/dolphinctl
go build -o bin\loadgen.exe ./cmd/loadgen

# 2. 一键完整闭环压测（环境检查→网关压测→批量建任务→等调度→抓指标→出报告）
.\hack\bench_full.ps1
```

报告自动保存到 `results/bench-report.txt`，包含：

| 阶段 | 数据 |
|------|------|
| 网关压测 | QPS、P50/P95/P99 延迟、错误率 |
| 任务创建 | 创建速率 tasks/sec |
| 调度 | dispatch 总数、调度延迟均值 |
| Worker 执行 | 完成数、平均执行耗时、按状态拆分 |
| 网关 | 总请求数、错误数 |

### 单独用 loadgen 压测

```bash
# 50 并发打 10 秒
loadgen -url http://localhost:8080/health -concurrency 50 -duration 10s

# 限制 1000 QPS 打 30 秒
loadgen -url http://localhost:8080/health -qps 1000 -duration 30s
```

---

## 三、需要实际跑出来的数据（面试汇报）

| 序号 | 数据点 | 说明 |
|------|--------|------|
| 1 | 网关 QPS + P99 延迟 | 证明网关吞吐能力 |
| 2 | 调度延迟 P99 | 证明"调度新鲜度 SLO < 2s"不是空话 |
| 3 | 任务创建吞吐 | 证明批量创建能力（dolphinctl stress create） |
| 4 | 故障恢复时间 | 证明 Failover SLO |
| 5 | 一次完整冒烟测试输出 | 证明端到端链路 |
| 6 | Worker 执行耗时 | 证明执行链路真实（如 39.7ms/次） |

跑完 `bench_full.ps1` 后，把 `results/bench-report.txt` 里的数字填进上文的表格，并更新 README 的压测章节。

---

## 四、常见问题排查

| 症状 | 原因 | 解决 |
|------|------|------|
| scheduler 报 `connect mysql failed` | MySQL 未启动 | `docker-compose up mysql`，等 healthcheck 变 healthy |
| scheduler 报 `connect etcd failed` | etcd 未启动 | `docker-compose up etcd` |
| worker 无日志 / 未注册 | scheduler 未启动 | 先启动 scheduler |
| worker 注册被拒 | gRPC 端口冲突 | 检查 50051 被占用 |
| gateway 创建任务报错 | gateway 只做了代理，任务 API 在 scheduler 的 gRPC | 用 dolphinctl 直接连 scheduler:50051 |
| smoke 测试 task logs 为空 | Worker 没在执行 | 检查 worker 日志是否注册成功；检查任务 handler 指向的 URL 是否可达 |
| 调度延迟很高 | Reconciler 每 1 秒轮询一次 Informer | 属正常（设计上是事件驱动，但 Informer 轮询间隔 1s） |

---

## 五、脚本说明

| 脚本 | 用途 |
|------|------|
| `hack/smoke_test.sh` | 端到端冒烟：基础设施 → 建任务 → 触发 → 查日志 |
| `hack/bench_gateway.sh` | 网关 QPS/延迟压测（wrk/vegeta） |
| `hack/bench_schedule.sh` | 调度吞吐/延迟压测（批量建任务） |
| `hack/test_failover.sh` | 故障注入：kill Worker 测恢复时间 |
| `cmd/dolphinctl` | 命令行工具：任务管理 + 批量建任务/触发 |
