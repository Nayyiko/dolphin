# Dolphin 面试要点

## 30 秒项目介绍

"我做了一个分布式任务管理平台 Dolphin，架构上借鉴了 Kubernetes 的控制器模式。系统分三层：API 网关处理同步流量（Radix Tree 路由 + 令牌桶限流 + 三态熔断器），调度引擎处理异步任务执行（etcd 选主 + Informer + WorkQueue + Reconcile Loop），Worker 层执行任务（协程池 + gRPC 双向流）。整个系统有完整的可观测体系——定义了 4 个 SLO，Prometheus 指标覆盖全链路，Grafana 面板可视化。"

## 技术亮点速查

| 序号 | 技术点 | 切入问题 | 回答核心 |
|------|--------|---------|---------|
| 1 | Radix Tree 路由 | 为什么不用 map？ | 前缀压缩省内存，时间复杂度不受路由数量影响，支持参数/通配符 |
| 2 | 令牌桶限流 | 为什么用 Redis Lua？ | 保证读-计算-写原子性，多实例无竞态 |
| 3 | 熔断器三态 | 为什么需要 HalfOpen？ | 防止刚恢复就被打挂，渐进式探测恢复 |
| 4 | etcd 选主 | 为什么不用 Redis？ | Raft + Lease 防脑裂，Redlock 有争议 |
| 5 | Cron 引擎 | 为什么不用 robfig/cron？ | 库只做单机，我需要预算 next_run_at 存 DB 全局调度 |
| 6 | Informer | 为什么不用定时扫库？ | List+Watch 事件驱动，本地缓存，数据库负载 O(1) |
| 7 | WorkQueue | 失败重试怎么设计？ | dirty 去重 + processing 防并发 + 指数退避 |
| 8 | Reconcile Loop | 怎么保证不丢任务？ | 持续比对期望 vs 实际，失败限速重入队 |
| 9 | Conditions | 任务状态怎么定义？ | Type/Status/Reason 数组，多维独立，转换可追溯 |
| 10 | Finalizers | 删除时在执行怎么办？ | 先取消 Worker 执行 → 清理 → 移除终结器 → 真删 |
| 11 | gRPC 双向流 | 为什么不用 HTTP 轮询？ | Push 模型延迟低，心跳复用连接 |
| 12 | 协程池 | goroutine 池有必要吗？ | 防无界 goroutine，per-task context 超时 |
| 13 | SLO | 怎么衡量质量？ | 可用性 99.95%、调度新鲜度 P99<2s、成功率 99.9% |
| 14 | 负载均衡 | 为什么 least_conn？ | 轮询不考虑真实负载，心跳上报 current_load |
| 15 | CI/CD 流水线 | 为什么用 kind 做 E2E？ | 真实集群验证"可观测结果"而非"部署成功"；三阶段门禁 + 两个真实故障的排障见 [ci-cd.md](ci-cd.md) |

## 高频追问

### Q: 你的 Leader 选举脑裂怎么办？
etcd Lease 机制天然防脑裂。Leader 需持续续约，网络分区导致续约失败后 Lease 过期，etcd 拒绝旧 Leader 写入，新 Leader 已上任。

### Q: 任务分发怎么保证不丢？
两道保险：下发前先写 TaskLog(running)，Worker 挂了 Failover 能查到并重分配；Worker 执行完回传 Result，Scheduler 收到才更新状态，没收到通过心跳超时补偿。

### Q: 和 K8s CronJob 什么关系？
不同抽象层。K8s 管 Pod 调度（基础设施），Dolphin 管任务调度（应用业务）。K8s CronJob 每次执行冷启动一个 Pod，不支持负载均衡和分片。Dolphin 是 gRPC push 到运行中的 Worker。两者互补，Dolphin 跑在 K8s 上。

### Q: 系统瓶颈在哪？
MySQL task 表在百万任务时索引查询压力大（方案：按 status 分区）。单 Leader 的 Reconciler 是单点，但 1 秒间隔 + 本地缓存足以支撑万级 QPS。Worker 无状态可水平扩展。

## 基准数据

| Benchmark | 结果 |
|-----------|------|
| Radix 静态路由匹配 | ~92 ns/op |
| Radix 参数路由匹配 | ~290 ns/op |
| Cron Next() 计算 | ~663 ns/op, 0 alloc |
