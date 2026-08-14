# Dolphin 架构设计

## 设计目标

- **业务调度而非容器调度**：K8s CronJob 面向基础设施任务，Dolphin 面向应用业务任务。
- **借鉴 K8s 控制器模式**：Informer / WorkQueue / Reconcile Loop / Conditions / Finalizers。
- **工业级可观测**：SLO 定义、Prometheus 指标、Grafana Dashboard。

## 组件职责

| 组件 | 同步面/异步面 | 职责 |
|------|-------------|------|
| Gateway | 同步 | 路由、鉴权、限流、熔断、反向代理 |
| Scheduler | 异步 | 选主、Cron 解析、任务调度、故障转移 |
| Worker | 异步 | 协程池执行、心跳、结果上报 |

## 数据流

```
1. 用户 → Gateway（鉴权+限流）→ Scheduler（CreateTask）
2. Scheduler 存储 Task → Informer 感知 → EnqueueTask
3. Reconciler: needExecution? → 选 Worker → 下发
4. Worker 执行 → 上报结果 → 更新 TaskLog + Conditions
5. Worker 挂 → 心跳超时 → 重新分配 → 指数退避重试
```

## 关键设计决策

### 为什么用 etcd 做选主而非 Redis？
etcd Raft 共识 + Lease 过期提供更强的安全保证。Redis Redlock 存在时钟跳跃导致锁失效的争议。Lease 过期后旧 Leader 的写入被 etcd 拒绝 → 天然防脑裂。

### 为什么 Informer 不用 MySQL 轮询？
调度延迟从 1 秒不确定性变为事件驱动。数据库负载从 O(N) 降到 O(1)。最终一致性由 Reconcile Loop 兜底。

### 为什么任务状态用 Conditions 而非单字段？
多维状态独立变化。状态转换有 Reason/Message/TransitionAt 可追溯。对标 K8s Pod Conditions。

### 为什么 Worker 通信用 gRPC 双向流？
Push 模型延迟低。心跳复用同一连接。支持服务端主动下发任务和取消指令。

### 为什么重试用指数退避？
给下游恢复时间，避免雪崩。`baseDelay * 2^failures` 上限 maxDelay。

## DAG 依赖编排

任务通过 `depend_on` 声明上游，构成有向无环图。两个机制保证依赖正确且低延迟：

### 创建时环检测（安全面）
`CreateTask` / `UpdateTask` 时用 **Kahn 拓扑排序**校验全量依赖图，拒绝：
- 环（返回环路径，如 `[a, b, a]`）；
- 悬空引用（依赖不存在的任务）；
- 自依赖、非法策略。

拒绝发生在**写入数据库之前**，坏图永远进不了系统。

### 运行时依赖门控（正确性面）
任务到期后，Reconciler 先做依赖判定再决定是否下发：

- **新鲜度语义**：每个上游必须在「本任务上次运行之后」有符合策略的执行，依赖不会串到上一次运行的旧结果（Makefile 式语义）。
- **策略**：`all_success`（默认，上游成功）／ `all_completed`（上游完成即可，含失败/超时）。
- **挂起不推进周期**：依赖未满足时保持 `next_run_at` 不变，不错误推进调度周期，也不重复下发。

### 事件驱动唤醒（延迟面）
上游执行结果到达时，server 层回调 Reconciler 推送直接依赖它的下游任务——**毫秒级唤醒**，不必等 1s 轮询扫描。扫描器仍作为兜底（最终一致性）。

```
A 完成 → TaskResult → server 回调 → EnqueueDependents(B, C) → 下游重新判定依赖
```

### DAG 指标
- `dolphin_scheduler_dag_blocked_tasks`：当前被依赖阻塞的任务数（gauge）
- `dolphin_scheduler_dag_gate_total`：依赖门控次数（到期未满足被挂起）
- `dolphin_scheduler_dag_cycle_reject_total`：环检测拒绝次数

## SLO

| SLI | SLO |
|-----|-----|
| API 可用性 | 99.95% |
| 调度新鲜度 P99 | < 2s |
| 任务执行成功率 | > 99.9% |
| 故障转移恢复 P99 | < 30s |

## 可观测性

所有组件暴露 Prometheus `/metrics`：
- **Gateway**: requests_total, request_duration_seconds, errors_total, circuit_breaker_state
- **Scheduler**: tasks_total, task_lag_seconds, dispatch_total, queue_depth, is_leader
- **Worker**: tasks_executing, task_duration_seconds, pool_capacity_utilization

## 与 Kubernetes 的关系

Dolphin **不替代** K8s，而是**利用** K8s：

- K8s 管理 Pod 的调度与生命周期（基础设施层）。
- Dolphin 管理业务任务的触发与执行（应用层）。
- Dolphin 自身作为 K8s Deployment 部署，享受自愈、滚动更新、HPA。

```
K8s CronJob 适用场景：数据库备份、日志清理、巡检（低频、单实例、冷启动可接受）
Dolphin 适用场景：业务计算、报表生成、健康检查（高频、需负载均衡、低延迟）
```
