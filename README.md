# Dolphin — 分布式任务管理平台

借鉴 Kubernetes 架构思想（声明式 API、Informer、WorkQueue、Reconcile Loop、Conditions、Finalizers）构建的分布式定时任务调度系统。

## 架构

```
┌─────────────────────────────────┐
│        API Gateway (同步面)       │
│   Radix Tree 路由 → 中间件链      │
│  Auth / RateLimit / CircuitBreaker│
└──────────┬──────────────────────┘
           │ 管理 API (gRPC)
           ▼
┌─────────────────────────────────┐
│      Scheduler (控制面)          │
│  etcd 选主 → Informer → WorkQueue│
│  → Reconcile Loop → 分发         │
└──────────┬──────────────────────┘
           │ gRPC 双向流
           ▼
┌─────────────────────────────────┐
│        Worker (执行面)            │
│  协程池 → 执行 → 结果上报         │
└─────────────────────────────────┘

   etcd (选主/配置)   MySQL (持久化)   Redis (限流)
```

## 技术栈

| 组件 | 技术 |
|------|------|
| 语言 | Go 1.22+ |
| 选主 | etcd (Lease + Campaign) |
| 持久化 | MySQL 8 + GORM |
| 限流 | Redis Lua 令牌桶 |
| 通信 | gRPC 双向流 |
| 可观测 | Prometheus + Grafana |
| 部署 | Docker / K8s |

## 快速开始

### 1. 启动基础设施

```bash
docker-compose -f deployments/docker-compose.yaml up -d etcd mysql redis
```

### 2. 启动三个组件（三个终端）

```bash
make run-gateway
make run-scheduler
make run-worker
```

### 3. 创建定时任务

```bash
# 通过 gRPC 管理 API 创建任务（使用 grpcurl 或自定义客户端）
grpcurl -plaintext -d '{
  "name": "健康检查",
  "cron_expr": "*/5 * * * *",
  "handler": "http://localhost:9090/healthz",
  "handler_type": "http",
  "timeout": 10,
  "max_retries": 3
}' localhost:50051 dolphin.Scheduler/CreateTask
```

### 4. 查看指标

```bash
curl localhost:9090/metrics | grep dolphin
```

## 核心设计

### Informer（事件驱动缓存）
对标 k8s client-go informer：启动时 List 全量同步到本地内存，之后 Watch 增量变更。调度器读本地缓存而非数据库，数据库负载从 O(N) 降到 O(1)。

### WorkQueue（限速去重队列）
对标 k8s util/workqueue：dirty set 去重、processing set 防并发、指数退避 RateLimiter。失败任务按 `baseDelay * 2^failures` 延迟重入队。

### Reconcile Loop（持续协调）
对标 k8s Reconciler：每个控制周期比对"期望状态 vs 实际状态"。到期任务 → 选负载最低 Worker 下发 → 更新 next_run_at + Conditions。失败 → 限速重入队。

### Conditions（多维状态）
对标 k8s Pod Conditions：Type/Status/Reason/Message/TransitionAt 数组，多维状态独立变化。

### Finalizers（优雅删除）
删除任务时先通知 Worker 取消执行 → 标记日志 → 移除终结器 → 真正删除。

## 目录结构

```
dolphin/
├── cmd/                    # 三个组件入口
│   ├── gateway/           # API 网关
│   ├── scheduler/         # 调度引擎
│   └── worker/            # 执行器
├── internal/
│   ├── gateway/           # 路由/中间件/代理
│   ├── scheduler/         # 选主/cron/informer/queue/reconciler/manager
│   ├── worker/            # 协程池/gRPC 客户端
│   └── pkg/               # model/metrics/config/etcdutil/tracing
├── api/proto/             # gRPC 协议定义
├── configs/               # 配置文件
└── deployments/           # Docker/K8s 部署清单
```

## 测试与基准

```bash
make test        # 单元测试
make bench       # 基准测试
```

| Benchmark | 结果 |
|-----------|------|
| Radix 静态路由匹配 | ~92 ns/op |
| Radix 参数路由匹配 | ~290 ns/op |
| Cron Next() 计算 | ~663 ns/op, 0 alloc |

## 设计文档

- [架构设计](docs/architecture.md)
- [面试要点](docs/interview.md)

## License

MIT
