# Dolphin 项目描述（简历 + 面试素材）

> 配套 `docs/evidence-plan.md`（数字证据链）、`docs/ci-cd.md`（CI/CD 与 E2E 排障）使用。同一个项目两套定位：
> **投 Go 后端用 A 版（分布式系统）**，**投 SRE 用 B 版（可靠性工程）**。
> 所有数字均为实测（`hack/failover_test.ps1` / `loadgen` / CI），面试被追问可回证据链。

---

## 一、简历项目栏（两版任选）

### A. Go 后端版 —— 标题：Dolphin 分布式任务调度平台

> Go · etcd · MySQL · Redis · gRPC · Prometheus

- 借鉴 Kubernetes 控制器模式，用 Informer + WorkQueue + Reconcile 实现事件驱动调度，替代定时扫库；实测调度延迟 P50 约 1.4s / P99 约 2s。
- 基于 etcd Lease 实现多调度器选主，从机制上杜绝脑裂（Lease 过期后旧主写入被拒绝）；实测 Leader 故障接管 10 次：11.2–15.64s（中位 13.35s），落在 [10,15]s 理论窗口。
- 通过幂等 instance_id + 心跳超时重分发实现任务不丢不重；实测 Worker 故障恢复 5 次：22.68–29.4s，幂等 5 次全 PASS。
- 用 Redis + Lua 实现分布式令牌桶限流，多网关实例共享限流状态；实测 15000 请求中 429 数量与理论超限值吻合。
- 全链路 Prometheus 埋点（调度延迟、选主状态、队列深度、分发计数），`go test -race` 全过。
- 搭 GitHub Actions 三阶段流水线（vet + race 单测 + 覆盖率门禁 → 多阶段镜像构建 → kind 真实集群 E2E），E2E 验证网关可达 / 双调度器单主 / 任务真实执行，core 逻辑包覆盖率 56.8%；CI 抓出并修复 MySQL 就绪探针 socket 路径 bug。

### B. SRE 版 —— 标题：Dolphin 高可用任务调度平台

> Go · etcd · Prometheus · 故障注入测试 · 压测

- 定义并实测调度系统核心 SLO：Leader 故障接管 10 次 11.2–15.64s（中位 13.35s）、Worker 故障恢复 5 次 22.68–29.4s、幂等 5 次全 PASS。
- 搭建故障注入测试体系（kill leader / kill worker / 慢任务），验证故障转移链路端到端闭环。
- 埋点 Prometheus 黄金信号与业务指标（选主状态、调度延迟、队列深度、worker 在线数），支撑排障与容量判断。
- 排查「假脑裂」故障：定位为观测手段错误（指标子串匹配命中 HELP 行）而非系统故障，形成"先确认系统坏了还是仪表盘错了"的排障方法论。
- 搭 GitHub Actions + kind 集群 CI/CD（三阶段门禁 + E2E 验证选主/执行）；排掉两个真实故障——MySQL 就绪探针 socket 路径错误、kind 冷启动 scheduler 就绪超时抖动（容错放宽超时 + 逐依赖日志兜底）。

---

## 二、1 分钟项目介绍（面试开场）

> 通用版，两方向都能用，最后一句按方向切换。

"我做了一个受 Kubernetes 控制器模式启发的 Go 分布式任务调度平台。核心不是定时扫库，而是 Informer 维护本地缓存 + WorkQueue 限速重试 + Reconcile 持续协调。调度器高可用我用 etcd 选主——关键不是'拿到锁'，而是旧 Leader 的 Lease 过期后写入被拒绝，从机制上防脑裂；任务执行侧用幂等 instance_id 保证 worker 挂了不丢不重。整个系统我做了 Prometheus 埋点和故障注入测试，实测 Leader 接管 10–15 秒、Worker 恢复 22–29 秒、幂等验证通过。"

（Go 后端补一句）"这个项目让我把 Go 的并发、etcd、gRPC、分布式一致性真正串起来跑通了。"
（SRE 补一句）"这个项目让我体会到，可靠性不是靠运气，是靠定义 SLO 并持续用故障注入去验证它。"
（聊工程方法补一句，可选）"我还给它配了完整 CI/CD——单测门禁、多阶段镜像构建、kind 真实集群 E2E，CI 抓出并修掉了两个真实故障。"

---

## 三、STAR 故事：「假脑裂」排障（面试"讲一个你解决的难题"备用）

**S（情境）**
我在写 Leader 故障转移测试时，测出来两个 scheduler 同时报 `is_leader=1`，接管时间只有 1.17 秒——看起来像严重的脑裂。

**T（任务）**
要判断到底是选主逻辑真的错了（真脑裂），还是我的观测手段出了问题（假象）。这决定我是改选主代码，还是改测试脚本。

**A（行动）**
我没有急着动选主代码，而是先做了三件事：① 通读 etcd v3.7.1 `concurrency` 源码，确认竞选 key 由 leaseID 唯一确定、`waitDeletes` 按 create_revision 全序等待，etcd 层数学上不可能双主；② dump etcd 实际状态，发现任意时刻只有 1 个 leader key、1 个 lease；③ 回头查测试脚本，发现它读 Prometheus 指标时用子串匹配，命中了 `# HELP ... is_leader 1` 帮助行的文字，把 is_leader 永远读成了 1。

**R（结果）**
改成行首锚定匹配后，假双主消失，接管时间回到 10–15s 的理论窗口（10 次实测 11.2–15.64s）。我从此记住了一条排障方法论：分布式系统出问题，第一步是确认"系统真的坏了"，而不是"仪表盘读错了"。

> 这个故事同时打两个方向：Go 后端面试展示"我能读懂 etcd 源码定位问题"，SRE 面试展示"我的排障方法论"。

---

## 四、STAR 备选：「CI E2E 抓出的两个真实故障」（投 SRE / 聊工程方法时好用）

**S（情境）**
我给 Dolphin 配了 GitHub Actions 三阶段流水线（单测门禁 → 镜像构建 → kind 真实集群 E2E）。E2E 里 mysql pod 一直不 Ready，日志刷了 34 次 readiness probe failed。

**T（任务）**
判断是 mysql 镜像、部署配置还是探针本身的问题，让集群能真正部署、CI 可复现，而不是"换个环境就过"。

**A（行动）**
我没急着改部署，先看 probe 失败日志：探针是 `mysqladmin ping -h localhost`——`-h localhost` 走 Unix socket，而 mysql:8.0 的 socket 实际在 `/var/lib/mysql/mysql.sock`，默认路径下找不到。改成 TCP 探测 `-h 127.0.0.1 -P 3306`（与应用、init 容器一致）后 pod 正常 Ready。另一次是 scheduler 就绪超时：用"主进程第一行日志时间戳"定位到 init 容器在依赖已 Ready 后仍轮询了 ~2m17s（kind 冷启动抖动），把就绪等待 150s 放宽到 300s、并给 init 容器加逐依赖日志，让下次卡住能直接看出在等谁。

**R（结果）**
两个故障都闭环：mysql 探针改 TCP 后 CI run #18 全绿，scheduler 超时放宽后 run #22 全绿。留下的方法：集群环境会暴露单机复现不了的 bug（socket 路径这种）；CI 里的偶发抖动，要"给足容错 + 失败可自解释"，而不是把超时设到恰好能过。

> 这个故事展示的是"给系统配工程闭环并用 CI 抓真实故障"，比"假脑裂"更适合回答"讲一个你遇到的真实故障"——那两个确实是系统/环境的真 bug，不是观测误报。

---

## 五、可复用的一句话卖点（投递话术）

- 投 Go 后端：**"我做过一个带选主和故障转移的分布式调度平台，不是 CRUD——我知道一致性、幂等、高可用这些词背后代码长什么样。"**
- 投 SRE：**"我给自己的调度系统定义了 SLO 并实测验证——Leader 接管 11.2–15.6s、Worker 恢复 22.7–29.4s、幂等 5 次全 PASS，还踩过一个假脑裂的坑学会了先怀疑观测。"**
- 聊工程方法：**"Dolphin 不只是能跑，还配了三阶段 CI/CD + kind 集群 E2E，期间 CI 抓出并修掉两个真实故障——MySQL 探针 socket 路径、冷启动就绪超时。"**
