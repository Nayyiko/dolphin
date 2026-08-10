# Dolphin vs Kubernetes CronJob — 论证指南

> 面试常问："你这个调度器和 K8s CronJob 有什么区别？凭什么说你更好？"
> 本文给出：核心立场、可测量对比维度、本地对照实验设计、数据采集表、面试话术。

---

## 一、核心立场（先说清，否则全盘皆输）

**Dolphin 不是"更好的 K8s CronJob"，而是"不同抽象层上的调度器"。**

这两者解决的是不同问题：

```
K8s CronJob     → 管理"基础设施级工作负载"的调度
                 (Pod 的创建、镜像拉取、容器生命周期)

Dolphin         → 管理"业务级任务"的调度
                 (HTTP 调用、shell 命令、gRPC 调用、执行状态追踪)
```

所以正确的论证不是"我比你快"，而是：
**"在业务级调度这个场景下，Dolphin 有四个可测量的差异，这些差异来自架构设计，而不是巧合。"**

面试时如果直接说"我比 CronJob 好"，会被追问到漏洞。说"我解决的是 CronJob 不适用的场景，且能用数据证明"——这才是 senior 的水准。

---

## 二、四个可测量对比维度

### 维度 1：调度延迟（Schedule Latency）

**K8s CronJob 链路：**
```
cron 触发 → API Server 创建 Job → 调度器 (kube-scheduler) 选节点
→ 拉镜像 → 创建 Pod → 容器启动 → 业务代码开始执行
```

**Dolphin 链路：**
```
cron 到期 → Reconciler 感知 → gRPC 推送到常驻 Worker → 协程池直接执行
```

| 环节 | K8s CronJob | Dolphin |
|------|------------|---------|
| 触发到执行开始 | 秒级 (Pod 冷启动) | 毫秒级 (gRPC push) |
| 镜像拉取 | 需要 (即使已缓存也有开销) | 不需要 (二进制已运行) |
| 容器启动 | 需要 (容器运行时创建进程) | 不需要 (goroutine) |
| 总延迟 | **5-30s** | **< 1s** |

**可测实验：** 两种方式各创建一个"立即执行"的任务，记录从触发到业务代码开始执行的时间。

### 维度 2：任务执行开销（Per-Execution Overhead）

| 指标 | K8s CronJob | Dolphin |
|------|------------|---------|
| 单次执行资源 | 1 个 Pod (约 20-100MB 内存) | 1 个 goroutine (约 2-4KB) |
| 高频场景 (每分钟) | 1 天 1440 个 Pod 创建销毁 | 1440 次 goroutine 调度 |
| 基础设施压力 | etcd + API Server + 调度器 + 容器运行时 | 复用常驻连接 |

**可测实验：** 创建 100 个每 1 分钟执行的任务，观察:
- K8s: `kubectl get pods` 的创建/销毁频率，API Server 负载
- Dolphin: Worker 进程的 CPU/内存曲线

### 维度 3：负载感知调度（Load-Aware Scheduling）

**K8s CronJob 不感知目标服务的负载。** 一个 CronJob 就是一个独立的 Pod，调度器只看节点资源（CPU/内存），不看这个任务要调用的下游服务是否过载。

**Dolphin 的 Worker 心跳上报 current_load，调度器选择负载最低的 Worker。** 这是 K8s CronJob 完全没有的能力。

**可测实验：**
1. 启动 2 个 Worker A/B
2. 给 Worker A 塞入大量慢任务，使其 current_load 高
3. 用 Dolphin 创建任务 → 观察调度器是否自动选 B
4. `dolphinctl task logs` 看 worker_id 分布

### 维度 4：执行可观测性（Observability）

| 能力 | K8s CronJob | Dolphin |
|------|------------|---------|
| 执行历史 | 散落在各节点 Pod 日志 | 结构化 task_logs 表 |
| 失败原因 | 看 Pod 状态 + 日志 | error_msg 字段 + Conditions |
| 重试策略 | backoffLimit (简单次数) | 指数退避 + 逐 instance 追踪 |
| 手动触发 | `kubectl create job --from=cronjob` | `dolphinctl task trigger` |
| 执行追踪 | 分散 | trace_id 贯穿 |

---

## 三、本地对照实验设计

### 准备：起一个单节点 K8s（kind）

```bash
# 需要 Docker Desktop
brew install kind   # macOS
# 或 curl -Lo kind https://kind.sigs.k8s.io/dl/v0.24.0/kind-linux-amd64

kind create cluster --name demo
```

### 实验 A：调度延迟对比

**K8s CronJob 侧：**
```bash
cat <<'EOF' | kubectl apply -f -
apiVersion: batch/v1
kind: CronJob
metadata:
  name: echo-job
spec:
  schedule: "*/1 * * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: echo
            image: busybox
            command: ["sh", "-c", "date +%s%3N >> /tmp/start.txt && echo done"]
          restartPolicy: OnFailure
EOF

# 观察从 Job 创建到 Pod 启动的时间
kubectl get jobs -w
kubectl get pods -w
```

**Dolphin 侧：**
```bash
./bin/dolphinctl task create \
  --name "echo-task" \
  --cron "*/1 * * * *" \
  --handler "http://localhost:9090/echo" \
  --type http

./bin/dolphinctl task logs --id <TASK_ID>
```

**数据记录：**

| 实验 | 触发到开始执行 | 备注 |
|------|--------------|------|
| K8s CronJob | ____ ms | 含 Pod 创建+镜像+启动 |
| Dolphin | ____ ms | 纯 gRPC push |

### 实验 B：高频任务下基础设施压力

```bash
# K8s 侧：创建 100 个每分钟 CronJob
for i in $(seq 1 100); do
  kubectl create cronjob cj-$i --image=busybox --schedule="*/1 * * * *" -- sh -c "echo hi"
done
kubectl get pods | wc -l   # 观察 Pod 数量波动

# Dolphin 侧：
./bin/dolphinctl stress create --count 100 --prefix demo --cron "*/1 * * * *" --handler "http://localhost:9090/healthz"
top / 任务管理器    # 观察 Worker 进程 vs K8s 的 etcd/API Server 负载
```

### 实验 C：负载感知

```bash
# 启动 2 个 Worker（不同终端）
make run-worker   # Worker 1
# 修改 worker.yaml 端口后
make run-worker   # Worker 2

# 给 Worker 1 塞慢任务（或直接看调度日志）
./bin/dolphinctl stress create --count 10 --prefix slow --handler "http://localhost:9999/slow"

# 创建新任务，看是否被分到负载低的 Worker
./bin/dolphinctl task logs --id <TASK_ID>   # worker_id 字段
```

---

## 四、面试话术

### 主动版（在项目介绍后顺势引出）

> "这个项目和 K8s CronJob 的关系，我一般这样解释：K8s CronJob 解决的是基础设施任务的调度——每次执行都要创建一个 Pod，秒级延迟，适合数据库备份、日志清理这种低频任务。Dolphin 解决的是业务任务的调度——Worker 常驻，gRPC 推送，毫秒级延迟，适合高频、需要追踪执行状态、需要负载感知的任务。它们不在同一抽象层，K8s CronJob 管 Pod 放哪，Dolphin 管任务何时跑、跑在哪、跑得怎么样。"

### 被问版（面试官直接问"凭什么比 CronJob 好"）

三步回答：

**第一步，划清边界：**
"首先，我不认为 Dolphin 在所有场景都优于 CronJob。如果任务是低频、单实例、每次需要隔离环境（比如跑一个独立的数据处理 Job），CronJob 更合适，因为它有完整的 Pod 隔离和安全边界。"

**第二步，给出可测量差异：**
"但在业务级高频调度场景，Dolphin 有四个可测量的差异：第一，调度延迟——CronJob 要创建 Pod、拉镜像、启动容器，秒级；Dolphin 是 gRPC 推给常驻 Worker，毫秒级。第二，执行开销——CronJob 每次一个 Pod，Dolphin 每次一个 goroutine。第三，负载感知——CronJob 不感知下游负载，Dolphin 会选负载最低的 Worker。第四，可观测性——CronJob 的执行历史散在 Pod 日志里，Dolphin 有结构化的 task_logs 和 Conditions。"

**第三步，给出诚实的局限：**
"当然这些差异我是在单机多进程环境验证的。真实的分布式网络环境下，延迟差异会变小，但架构层面的差异——常驻 Worker vs 每次冷启动 Pod——是本质性的。"

---

## 五、一句话总结（背下来）

> "Dolphin 不是替代 K8s CronJob 的，是解决它不适用的场景的。CronJob 管理基础设施级任务，Dolphin 管理业务级任务。Dolphin 的 Worker 常驻、毫秒级延迟、负载感知、结构化可观测——这些是 CronJob 架构上做不到的，也是我选择自己造轮子的原因。"
