# CLAUDE.md — Claude Code 编码指南

## 你是谁

你是一个资深 Go 后端工程师，正在帮助搭建 Dolphin——一个借鉴 Kubernetes 架构思想的分布式任务管理平台。

## 项目是什么

Dolphin 是一个分布式任务调度系统，分三层：
- **API Gateway（同步面）**：HTTP 网关，Radix Tree 路由 + 中间件链 + 令牌桶限流 + 熔断器
- **Scheduler（控制面）**：etcd Leader 选举 + Cron 引擎 + Informer/WorkQueue/Reconciler + 任务分发
- **Worker（执行面）**：gRPC 客户端 + 协程池 + Context 超时控制 + 心跳上报

架构思想借鉴 Kubernetes：Informer（事件驱动缓存）、WorkQueue（限速去重队列）、Reconcile Loop（持续协调）、Conditions（多维状态）、Finalizers（优雅删除）。

基础设施：etcd（选主 + 配置）、MySQL（任务持久化）、Redis（限流计数）

## 关键引入

```
go.etcd.io/etcd/client/v3        — etcd 客户端
google.golang.org/grpc            — gRPC 框架
google.golang.org/protobuf        — protobuf
github.com/prometheus/client_golang — Prometheus 指标
github.com/go-sql-driver/mysql    — MySQL 驱动
github.com/redis/go-redis/v9      — Redis 客户端
gorm.io/gorm                      — ORM
github.com/golang-jwt/jwt/v5      — JWT 鉴权
github.com/google/uuid            — UUID 生成
golang.org/x/time                 — 限流器（令牌桶）
```

## 开发排期（5 天，AI coding 加速版）

### Day 1 — 脚手架 + 共享层 + 基础跑通
- go mod 初始化，项目骨架搭建
- model 层（Task + Conditions + TaskLog + Worker）
- etcd client 封装（连接、Watch、Lease、CAS）
- docker-compose（etcd + MySQL + Redis）
- configs 目录下的三个 yaml 配置文件
- 三个 cmd 入口能编译启动（空服务）

### Day 2 — API Gateway
- Radix Tree 路由实现（Insert、Search——节点分裂、前缀压缩、路径参数和通配符）
- Context 对象（Params/Keys/Next/Abort）
- 中间件链（Chain 构建器、洋葱模型执行）
- Auth 中间件（JWT 鉴权）
- RateLimit 中间件（Redis Lua 令牌桶，原子操作）
- Recovery 中间件（Panic 恢复）
- Logger 中间件（请求日志）
- Tracing 中间件（Trace ID 注入与透传）
- CircuitBreaker（三态状态机：Closed → Open via failures, Open → HalfOpen via timeout, HalfOpen → Closed/Open via probe success/failure）
- ReverseProxy（基于 httputil.ReverseProxy，自定义 Transport 连接池）
- LoadBalancer（Round Robin 和 Weighted Round Robin）
- 优雅关闭（SIGTERM → drain 连接）
- /health 和 /metrics 端点
- 路由单元测试（覆盖静态路由、路径参数、通配符、404）

### Day 3 — Scheduler 核心
- Cron 5-field 解析器 + Next() 时间计算 + 单元测试（覆盖简单、复杂、边界情况）
- etcd Leader 选举（Campaign、Session 保活、故障重选）
- Task CRUD（gRPC handler）
- Informer 实现（List + Watch → 本地缓存 Lister）
- WorkQueue 实现（去重 dirty set + RateLimiter 接口 + 指数退避 ItemExponentialFailureRateLimiter）

### Day 4 — Reconciler + Worker + gRPC 打通
- Reconciler（reconcile 方法 → 期望 vs 实际比对 → needExecution 判断 → 调 Executor 分发）
- Finalizer（deletion_timestamp 检测 → 通知 Worker 取消 → 清理日志 → 移除 finalizer → 真删）
- Conditions 状态管理（Type/Status/Reason/Message/TransitionAt）
- Worker 协程池（固定数量 goroutine + buffered channel + per-task context.WithTimeout）
- Worker 注册 + 心跳 + 任务执行结果上报（gRPC 双向流）
- gRPC 双向流 Server 端实现
- 端到端打通过（创建任务 → 调度 → Worker 执行 → 结果）

### Day 5 — Prometheus + CI/CD + 文档
- 所有 Prometheus 指标埋点（gateway/scheduler/worker 全量指标）
- Makefile（proto/lint/test/bench/build/docker/deploy-k8s/clean 目标）
- GitHub Actions CI/CD（lint + test + integration-test + docker-build-push + deploy 四阶段）
- Dockerfile（多阶段构建，<20MB，非 root 用户）
- K8s 部署清单（Deployment + Service + HPA）
- README.md + architecture.md + interview.md

## 编码准则

1. **Go 惯用**：遵循 Effective Go，使用 gofmt/goimports
2. **错误处理**：永远不忽略 error，使用 `fmt.Errorf("context: %w", err)` 包装
3. **并发安全**：共享状态加锁，channel 用于通信，sync.WaitGroup 等待 goroutine
4. **Context 传递**：所有 RPC/DB/Redis 调用都传 ctx，超时控制用 context.WithTimeout
5. **零依赖偏好**：优先用标准库，第三方库慎重——目前 go.mod 中列出的可以放心用
6. **可测试**：接口化（方便 mock），每个核心模块写单元测试
7. **可观测**：所有关键路径埋 Prometheus 指标
8. **优雅关闭**：所有服务响应 SIGTERM/SIGINT，drain 连接后再退出
9. **非 root 运行**：Dockerfile 中创建 dolphin 用户
10. **结构化日志**：使用结构化的 key=value 格式日志（log/slog 标准库即可）

## 命名惯例

- 包名小写，简短：`router` 而非 `router_package`
- 接口名用 `-er`：`RateLimiter`、`TaskLister`、`TaskExecutor`
- 导出符号 PascalCase：`LeaderElector`
- 未导出符号 camelCase：`watchSession`
- 文件名 snake_case：`circuit_breaker.go`
- 测试文件 `*_test.go`
- 常量用 PascalCase：`ConditionScheduled`

## 文件组织

- `cmd/` — 只有 main.go，无业务逻辑
- `internal/` — 所有业务逻辑，外部不可引入
- `pkg/` — 可被项目内复用的工具包
- `api/proto/` — protobuf 定义
- `configs/` — YAML 配置
- `deployments/` — Docker/K8s 部署清单
- `hack/` — 脚本和工具
- `tests/` — 集成测试
