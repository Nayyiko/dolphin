# Dolphin CI/CD 流水线（面试材料）

> 目标：把 GitHub Actions 三阶段流水线讲成「能证明工程能力」的故事——不只是"配了 CI"，而是每个 gate 为什么这么设计、E2E 在真实集群里验证了什么、期间排掉了哪些真实故障。
> 配套：本地 kind 手动验证清单见 [k8s-kind-check.md](k8s-kind-check.md)；创新点证据链见 [evidence-plan.md](evidence-plan.md)。

---

## 一句话总结

"一个 Go 分布式调度系统的 CI/CD：3 个 job 串成门禁——**静态检查+单元测试+覆盖率门禁** → **4 个多阶段 Docker 镜像可构建** → **在 kind 真实集群里跑完整 E2E**（网关可达 / 双调度器选主单主 / 任务真实执行），期间用 CI 抓出并修掉了一个 MySQL 就绪探针的真实故障。"

---

## 流水线全貌

文件：`.github/workflows/ci.yml`，触发于 `push (main/master)`、`pull_request`、`workflow_dispatch`。

```
push / PR
   │
   ▼
┌─────────────────────┐   ┌─────────────────────┐   ┌──────────────────────────┐
│ ① lint-and-test     │──▶│ ② build-images      │──▶│ ③ k8s-e2e (kind)         │
│ go vet ./...        │   │ 构建 4 个 Docker 镜像│   │ 起 kind → 加载镜像 → 部署 │
│ go test -race ./... │   │ gateway/scheduler/  │   │ V1 网关可达 (8080/health) │
│ 覆盖率门禁 (50%)    │   │ worker/dolphinctl   │   │ V2 选主单主 (is_leader=1) │
│ go build 5 个二进制 │   │                     │   │ V3 任务真实执行 (10/10)   │
└─────────────────────┘   └─────────────────────┘   └──────────────────────────┘
     needs: —                    needs: ①                     needs: ①②
```

- `concurrency: group ci-${{ github.ref }}, cancel-in-progress: true`：同一分支的新提交自动取消旧跑，节省排队时间（面试点：CI 成本控制）。
- 三个 job 串行有依赖关系：测试不过不构建镜像、镜像构建不过不跑 E2E——**门禁分层，快速失败**。

---

## Stage 1 · lint-and-test（快速失败门禁）

| Step | 命令 | 验证什么 |
|------|------|---------|
| Go Vet | `go vet ./...` | 静态检查：可疑构造、锁拷贝、格式串不匹配等 |
| 单元测试 | `go test -race -count=1 ./...` | 全量测试 + **race detector**（并发 bug 检测） |
| 覆盖率门禁 | `go test -race -coverprofile=coverage.out $COVER_PKGS` + `go tool cover` | 核心逻辑包聚合覆盖率 ≥ 50% |
| 构建检查 | `go build ./...` + 显式 build 4 个 cmd | 保证产物可编译 |

### 覆盖率门禁的设计决策（面试重点）

`COVER_PKGS` 不是 `./...` 全量，而是**精挑的核心逻辑包**：

```yaml
./internal/gateway/middleware ./internal/gateway/proxy ./internal/gateway/router
./internal/pkg/ratelog
./internal/scheduler/cron ./internal/scheduler/dag ./internal/scheduler/executor
./internal/scheduler/informer ./internal/scheduler/queue ./internal/scheduler/reconciler
./internal/worker/executor
```

为什么这么选：

- **排除 `cmd/*`**：进程入口，只有 `main` 调用装配，没有可测逻辑，会稀释真实覆盖率。
- **排除 `api/proto/pb`**：protoc 生成代码，天然无测试。
- **排除连接型集成包**（`election/manager/redisutil/etcdutil/config/metrics/tracing/model`）：它们测的是与外部服务（etcd/Redis/MySQL）的交互，本地单测写不满，属于集成测试范畴。
- **阈值 50%，本地实测 56.8%**：门禁卡在"真的有测"而不是"数字好看"，留了余量防抖动。

> **面试口径**："覆盖率门禁我刻意只统计核心逻辑包——把生成代码、进程入口、外部依赖交互排除掉，避免稀释。阈值定 50%，本地复现 56.8%，既保证核心算法（cron 解析、Radix 路由、Informer/WorkQueue/Reconciler）有测试兜底，又不会因为集成包测不满而频繁红。"

---

## Stage 2 · build-images（镜像可构建门禁）

对 4 个组件分别构建镜像并 `docker image inspect` 验证产物存在：

- `dolphin-gateway` / `dolphin-scheduler` / `dolphin-worker` / `dolphinctl`
- 全部**多阶段构建**：`golang:1.26-alpine` 编译 → `alpine:3.20` 运行时
- 运行时镜像特点（面试点）：
  - `CGO_ENABLED=0` 静态编译，最终镜像小（纯 CLI dolphinctl 尤其小）
  - **非 root 运行**：`adduser -D dolphin` + `USER dolphin`
  - 内置 `curl` + `HEALTHCHECK`，探活不依赖外部
  - `-trimpath` + `-ldflags="-s -w"` 去掉调试符号；gateway/scheduler/worker 注入 `Version/BuildTime/GitCommit`

> **面试口径**："镜像全是多阶段构建，builder 阶段塞进 git + 完整工具链，runtime 阶段只留 ca-certificates、tzdata、curl 和二进制，非 root 用户跑。既保证可复现构建，又控制攻击面。"

---

## Stage 3 · k8s-e2e（真实集群验证）⭐ 最值得讲

跑 `bash hack/k8s_ci_e2e.sh`（Linux 版；Windows 等价物 `hack/k8s_kind_up.ps1`）。整个流程：

```
前置检查 → 起 kind → 装默认 StorageClass → 构建/加载 4 镜像
→ 预拉基础设施镜像 → kubectl apply -k → 逐个等 Ready
→ V1 网关可达 → V2 选主单主 → V3 任务真实执行
```

### 为什么用 kind 而不是 minikube / 真实集群

- **kind** = Docker in Docker 的 K8s，CI runner 上无需特权，秒级起集群；镜像直接 `kind load docker-image` 进节点，**不依赖 registry**——PR 里的代码改动能端到端验证。
- 真实集群留给生产，minikube 在 CI 里重、慢、依赖虚拟化。

### 三个验证，正好对应三个核心能力

| 验证 | 怎么做 | 证明什么 |
|------|--------|---------|
| **V1 网关可达** | port-forward 8080 → `curl /health` 等 200 | 同步面（gateway）部署成功、Service 网络通 |
| **V2 选主单主** | 逐个 exec 进 2 个 scheduler pod，curl 9090/metrics，数 `dolphin_scheduler_is_leader 1` 的个数恰为 1 | etcd Lease 选主生效、**无脑裂** |
| **V3 任务真实执行** | `dolphinctl stress create` 建 10 个独立任务 → `task trigger-batch` 全触发 → 汇总 worker 的 `dolphin_worker_task_completed_total` 增量 ≥10 | 全链路：任务建 → 调度 → **worker 经 etcd 找到真实 Leader 并执行**（advertise_addr 生效） |

> **面试口径**："E2E 不是'部署成功就过'，而是验证三个可观测结果：网关 /health 返回 200、双调度器恰好 1 个 Leader、10 个任务真实执行完成。第三个验证尤其关键——它证明 worker 是通过 etcd 发现真实 Leader 地址去执行的，也就是 advertise_addr 注入在集群里真的生效了。"

### E2E 里的工程细节（全是面试料）

1. **失败不盲等，失败时打印诊断**：`wait_ready` 超时后输出 `get pods -o wide` + `describe pod` + `logs --tail=200`，让 CI 失败可自解释。
2. **冷环境预拉基础设施镜像**：CI runner 首次冷跑时，kubelet 才从 Docker Hub/quay 拉 mysql/etcd/redis，冷拉大镜像 + 匿名拉取限流（`toomanyrequests`）曾实测 mysql 360s 起不来 → 预拉进 runner Docker 再 `kind load`，运行时零外网依赖。
3. **`kubectl apply -k deployments/k8s`**：用 Kustomize 组织全部清单（namespace + etcd + mysql + redis + configmap + 三组件），一条命令幂等部署。
4. **V3 的坑：不能用 `stress trigger --id X --count N`**：它只是把同一任务 `next_run_at` 刷 N 次，informer 只见一次变更 → 等效只执行 1 次（曾跑出 4/450 的假象）。正确做法是建 N 个独立任务再 `trigger-batch`。
5. **探针修复故事（真实故障，CI 抓出来的）**：mysql readinessProbe 原为 `mysqladmin ping -h localhost`——mysql:8.0 的 socket 实际在 `/var/lib/mysql/mysql.sock`，而 `-h localhost` 找 `/var/run/mysqld/mysqld.sock`，导致 pod 永远不 Ready（日志 34 次 probe failed）。改为 `-h 127.0.0.1 -P 3306` 走 TCP，与应用/initContainer 一致。**CI run #18 全绿**。

> **面试口径**："最典型的 CI 排障：集群里 mysql 一直不 Ready，日志显示 34 次 readiness probe failed。根因是探针用 `-h localhost` 走了 Unix socket 路径，而 mysql:8.0 的 socket 不在默认位置——改成 TCP 127.0.0.1:3306 后 pod 正常 Ready。这个 bug 单机 docker-compose 复现不了，是上集群才暴露的，正好体现 E2E 的价值。"

---

## 面试追问准备

### Q: 为什么选 kind 做 E2E，而不是连一个真实集群？
CI 里真实集群不可得也不可重复；kind 在 Docker 内跑完整 K8s 控制面，免特权、秒级起、可销毁。且镜像 `kind load` 直接进节点，不依赖 registry，PR 代码能真实验证。代价是单节点、无法测跨节点网络分区——这个边界我会主动说明。

### Q: 覆盖率门禁为什么不是全量？
全量会被生成代码（proto）、进程入口（cmd）、外部依赖交互包稀释。我按包精挑核心逻辑，阈值 50%（实测 56.8%）保证"真的有测"，也避免集成包测不满导致的频繁红。

### Q: V2 选主验证会不会误报双主？
不会，指标行用行首锚定 `^dolphin_scheduler_is_leader ` 精确匹配数据行——早期踩过坑：用子串匹配会把 HELP 注释行误读成指标（假双主）。改成锚定后，只有真正 `is_leader=1` 的副本才计数，断言恰好 1 个。

### Q: 任务怎么验证"真实执行"了？
worker 暴露 `dolphin_worker_task_completed_total` 计数，脚本汇总所有 worker pod 的增量 ≥10 才过。这证明的不是"任务提交成功"，而是"任务被 worker 实际拉起来执行完并上报"。

### Q: CI 有缓存/推送镜像吗？
当前**只构建不推送**（无 registry push）；`kind load` 满足 E2E 需要，镜像发布留作部署阶段。这是明确的已知边界，也是可扩展点（接入 GHCR + 自动部署）。

---

## 已知边界与待改进

| 项 | 现状 | 可改进 |
|----|------|--------|
| 镜像推送 | 只 build 不 push | 接入 GHCR/Docker Hub + 语义化版本 tag |
| 部署 CD | 无自动部署 | E2E 绿后 Argo CD / kubectl 滚动更新到测试集群 |
| 覆盖率 | 聚合 56.8% 门禁 50% | 可加单包最低线，防"一个包拉满、其他全空" |
| kind 拓扑 | 单节点 | 多节点 kind 测跨节点网络/分区 |
| E2E 覆盖 | V1/V2/V3 | 可加 V4 背压 / V5 kill worker 重分发（本地版已有，见 k8s-kind-check.md） |
| 镜像预拉容错 | `kind load` 无条件执行，pull 失败会中断 | 改为"pull 失败则跳过 load，容忍运行时拉取" |

---

## 30 秒面试口径（背这句）

"Dolphin 的 CI/CD 分三个门禁：先 `go vet` + race 单元测试 + 覆盖率门禁（核心逻辑包 ≥50%），再构建 4 个多阶段、非 root 的 Docker 镜像，最后在 kind 真实集群里跑 E2E——验证网关可达、双调度器 etcd 选主单主、worker 经 etcd 发现真实 Leader 并执行 10 个任务。期间 CI 帮我抓出一个真实故障：mysql 探针 socket 路径不对导致 pod 永不 Ready，改成 TCP 探测后全绿。整个设计原则是快速失败、失败可诊断、E2E 验证的是可观测结果而不是'部署成功'。"
