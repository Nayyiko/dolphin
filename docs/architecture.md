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
