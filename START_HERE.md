# Dolphin — Claude Code 启动 Prompt

将以下内容**完整复制**到 Claude Code 对话中输入。

> 建议在 Claude Code 中 `/init` 后，将此 prompt 作为第一条消息发送。

---

## 使用方法

1. 打开 Claude Code，`cd` 进入项目目录：
   ```
   cd C:\Users\30641\Desktop\dolphin
   ```
2. 运行 `/init`（如果还没初始化 CLAUDE.md）
3. 将下面的 Prompt 一次性粘贴到对话中

---

## 复制整个 Prompt（从这行开始到下面 `END` 标记）

```
我正在构建一个 Go 项目：Dolphin — 分布式任务管理平台。

请先阅读项目中的 CLAUDE.md 了解完整上下文，然后严格按照 CLAUDE.md 中定义的开发排期和编码准则执行。

## 请你做的第一件事

1. 阅读项目中的 CLAUDE.md（已有，不要重新生成）
2. 检查 go.mod 中声明的依赖，执行 `go mod tidy && go mod download`
3. 确认项目骨架已存在（cmd/internal/api/configs/deployments/ 目录结构）
4. 如果目录不全，补齐 CLAUDE.md 中定义的全部目录结构
5. 告诉我"环境就绪，开始 Day 1"，我会确认后继续

## 开发方式

- 按 CLAUDE.md 中 Day 1 → Day 5 的顺序进行
- 每个 Day 结束后暂停，告诉我完成了什么、有哪些测试、覆盖率多少
- 严禁跳过任何阶段
- 每个模块写完立即写对应的单元测试
- 代码中使用英文注释，README/文档使用中文

## 技术规格参考

### 1. 共享数据模型（Day 1）

设计这些 MySQL 表结构：
- tasks：id, name, cron_expr, next_run_at, handler, handler_type(http/grpc/shell), params(JSON), timeout, max_retries, status(active/paused/deleted), created_at, updated_at
- task_logs：id, task_id, instance_id(UUID每次执行唯一), worker_id, status(running/success/failed/timeout/cancelled), start_time, end_time, result, error_msg, retry_count
- workers：id, address, status(online/offline), max_concurrency, current_load, last_heartbeat, registered_at
- task_conditions：id, task_id, type(Scheduled/Running/Healthy/Ready/Reconciling), status(True/False/Unknown), reason, message, observed_at, transition_at

etcd 路径约定：
- /dolphin/scheduler/leader — Leader 选举
- /dolphin/config/gateway — 网关配置（支持 Watch）
- /dolphin/tasks/{task_id} — 任务变更事件（Informer Watch）

### 2. API Gateway（Day 2）

Radix Tree 路由核心：
- 按 HTTP method 分树（map[string]*node）
- node 结构：path, indices（子节点首字符集合）, children, handler, isWild, paramName, wildChild
- Insert：去掉前导/，逐 segment 插入，前缀匹配时分裂节点
- Search：逐字符匹配 → 精准匹配优先 → 检查同级 wildChild → 提取 params

中间件链（洋葱模型）：
- Middleware = func(HandlerFunc) HandlerFunc
- Chain(handler, m1, m2, m3) → m1(m2(m3(handler)))
- Context：Request, Writer, Params, Keys(map[string]any), handlers, index, aborted
- Context.Next() 驱动责任链执行
- Context.Abort() 中断链

限流（Redis Lua 令牌桶）：
- KEY: ratelimit:{client_id}:{endpoint}
- HASH fields: tokens, last_refill
- 入参: rate(tokens/sec), capacity, now(timestamp)
- 原子操作：读 last_refill → 算补充 token → min(capacity) → 减 1 或拒绝

熔断器三态：
- StateClosed → 累计失败 >= threshold → StateOpen
- StateOpen → timeout 到期 → StateHalfOpen
- StateHalfOpen → 探测成功 count >= halfOpenMaxReqs → StateClosed
- StateHalfOpen → 任一探测失败 → StateOpen
- 读写锁：频繁读(Closed 状态)用 RLock，状态切换用 Lock

反向代理：
- 基于 httputil.ReverseProxy，自定义 Director(改 target URL + 注入 header) 和 Transport(连接池)
- Transport 配置：MaxIdleConns=100, MaxIdleConnsPerHost=10, IdleConnTimeout=90s
- LoadBalancer：Round Robin(atomic index)，扩展 Weighted Round Robin
- 优雅关闭：SIGTERM → 停止 Accept → drain 现有连接 → 退出

### 3. Scheduler（Day 3-4）

Cron 引擎：
- 5-field 格式：minute hour dayOfMonth month dayOfWeek
- fieldSpec：支持 values(map[int]bool), step(*/n), range(start-end)
- Next(t time.Time) time.Time 算法：
  1. t = t.Truncate(Minute) + 1min
  2. 逐级检查 Month → Day → Hour → Minute
  3. 不匹配则跳到下一级的最小值
  4. 最多循环 5 年防死循环

etcd Leader 选举：
- concurrency.NewSession(ttl) + concurrency.NewElection
- 后台 goroutine 每 TTL/3 续约
- watchSession：<-session.Done() → 标记 isLeader=false → 清理 → 重新 Campaign
- 脑裂防护：Lease 过期后 etcd 拒绝旧 Leader 写入

Informer（事件驱动缓存）：
- List（启动时全量） + Watch（增量 updated_at 轮询）
- 事件处理：OnAdd/OnUpdate/OnDelete → 更新本地 sync.Map → 推入 WorkQueue
- TaskLister 接口：List() / Get(id) / ListByStatus(status)
- 注意：这是最终一致性缓存，依赖 Reconcile 反复协调

WorkQueue（限速去重队列）：
- queue []string + dirty map（去重） + processing map（保护）
- RateLimiter 接口：When(item) Duration, Forget(item), NumRequeues(item) int
- ItemExponentialFailureRateLimiter：逐 key 指数退避 baseDelay * 2^failures（上限 maxDelay）
- Add/AddRateLimited/AddAfter/Get/Done 方法

Reconciler（持续协调核心）：
- 多个 worker goroutine 从 WorkQueue.Get 获取 taskID
- reconcile(taskID)：
  1. Lister.Get(taskID) 获取 TaskItem
  2. 检查 status != active → 跳过
  3. needExecution()：检查 next_run_at <= now 且无 running 实例
  4. 需要执行 → executor.Execute(选 Worker 下发) → 更新 next_run_at + Conditions
  5. 执行失败 → return error → WorkQueue.AddRateLimited
  6. 执行成功 → WorkQueue.Done
- needExecution 也检测 missed schedule（next_run_at 过期 > 5min 未执行）

Finalizer（优雅删除）：
- 删除请求只设 deletion_timestamp，不真删
- Reconcile 检测到 deletion_timestamp → 通知 Worker 取消 → 标记 task_log cancelled → 移除 finalizer → 真删

Conditions 状态管理：
- ConditionType: Scheduled/Running/Healthy/Ready/Reconciling
- ConditionStatus: True/False/Unknown
- 每次 reconcile 后更新 Conditions，TransitionAt 只在 Status 变化时更新

### 4. Worker（Day 4）

协程池：
- capacity 个常驻 goroutine，capacity*2 的 buffered task channel
- 每个任务独立 context.WithTimeout（不能 kill 整个 Worker）
- Submit：非阻塞写入 channel，满则拒绝
- execute：在单独 goroutine 执行 doExecute，select 监听 ctx.Done() 超时
- doExecute 内部所有阻塞点检查 ctx.Done()

gRPC 双向流：
- Worker 启动 → 连 Scheduler → 发送 RegisterReq
- 维持长连接，复用同一条流：
  - Worker→Scheduler: Heartbeat(worker_id + current_load), TaskResult
  - Scheduler→Worker: TaskDispatch, HeartbeatAck
- 心跳间隔 10s

### 5. Prometheus 指标（Day 5 — 贯穿全程）

每个模块写完后立即埋点，不等 Day 5 一起做：
- Gateway: requests_total, request_duration_seconds(Histogram), errors_total, requests_in_flight, ratelimit_rejected_total, circuit_breaker_state
- Scheduler: tasks_total, task_lag_seconds(Histogram), dispatch_total, leader_elections_total, is_leader, queue_depth, reconcile_duration_seconds, missed_schedules_total, failover_recovery_seconds
- Worker: tasks_executing, task_duration_seconds, task_completed_total, pool_capacity_utilization, heartbeat_latency_seconds, goroutines
- Infrastructure: etcd_connection_status, mysql_query_duration_seconds, redis_op_duration_seconds

### 6. 配置文件模板

configs/gateway.yaml：
```yaml
server:
  port: 8080
  read_timeout: 10s
  write_timeout: 30s
  idle_timeout: 120s
redis:
  addr: localhost:6379
  password: ""
  db: 0
jwt:
  secret: "change-me-in-production"
  expire_hours: 24
rate_limit:
  enabled: true
  default_rate: 100   # tokens/sec
  default_capacity: 200
scheduler:
  addr: localhost:50051
```

configs/scheduler.yaml：
```yaml
server:
  grpc_port: 50051
  http_port: 9090
etcd:
  endpoints:
    - localhost:2379
  dial_timeout: 5s
election:
  ttl: 15            # Lease TTL seconds
mysql:
  dsn: root:dolphin@tcp(localhost:3306)/dolphin?parseTime=true&charset=utf8mb4
reconciler:
  workers: 50        # 并行 reconcile goroutine 数（须足够高以打满 worker 池，触发背压重试）
failover:
  heartbeat_timeout: 30s
  max_retries: 3
```

configs/worker.yaml：
```yaml
server:
  metrics_port: 9091
scheduler:
  addr: localhost:50051
pool:
  capacity: 50       # 最大并发执行数
heartbeat:
  interval: 10s
```

## 最后提醒

- 永远先读 CLAUDE.md
- 每个模块写了代码就写测试
- 每个 Day 结束出可编译可运行的代码
- 不要一气呵成全部写完——分 Day，逐步交付
- 遇到技术选型问题，优先标准库，其次 go.mod 中已有的依赖

开始吧。先确认环境就绪。
```

---

END OF PROMPT

---

## 项目文件清单（已创建在 C:\Users\30641\Desktop\dolphin\）

```
dolphin/
├── CLAUDE.md           ← Claude Code 自动读取的指令文件
├── go.mod              ← 依赖声明（需要 go mod tidy 拉取）
├── .gitignore
├── cmd/
│   ├── gateway/
│   ├── scheduler/
│   ├── worker/
│   └── dolphinctl/
├── internal/
│   ├── gateway/
│   │   ├── router/
│   │   ├── middleware/
│   │   └── proxy/
│   ├── scheduler/
│   │   ├── election/
│   │   ├── cron/
│   │   ├── informer/
│   │   ├── queue/
│   │   ├── reconciler/
│   │   ├── manager/
│   │   └── executor/
│   ├── worker/
│   │   ├── executor/
│   │   └── heartbeat/
│   └── pkg/
│       ├── model/
│       ├── metrics/
│       ├── etcdutil/
│       ├── tracing/
│       └── errcode/
├── api/proto/
├── configs/
├── deployments/
│   ├── docker/
│   └── k8s/
├── hack/
├── docs/
└── tests/integration/
```

## 三步启动

1. 打开 Claude Code：
   ```bash
   cd C:\Users\30641\Desktop\dolphin
   ```

2. 运行 Claude Code 的 `/init`（可选——因为 CLAUDE.md 已存在，Claude Code 会自动读取）

3. 将上面 Prompt 完整粘贴到对话中

Claude Code 会按照 CLAUDE.md 中定义的 Day 1→5 排期、编码准则和技术规格逐步构建整个项目。
