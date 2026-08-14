# Dolphin on Kubernetes 部署指南

把 Dolphin 完整跑在 K8s 上：etcd / MySQL / Redis 作为基础设施，scheduler 做控制面（etcd 选主），worker 做执行面，gateway 做接入面。

## 架构

```
                 ┌──────────────┐   NodePort 30080
  外部用户 ─────▶ │  gateway x2  │◀──┐
                 └──────┬───────┘   │ 管理 gRPC
                        │ 50051      │
                 ┌──────▼───────┐   │
                 │ dolphin-scheduler │  (Service, 2 副本)
                 │  Service         │
                 └──────┬───────┘
           ┌────────────┼────────────┐
           ▼            ▼            ▼
   ┌────────────┐ ┌────────────┐ ┌────────────┐
   │ etcd       │ │ MySQL      │ │ Redis      │
   │ (选主/配置) │ │ (持久化)   │ │ (限流)     │
   └────────────┘ └────────────┘ └────────────┘
        ▲  Leader 地址写入 etcd（leader-addr）
        │
   ┌────┴───────────┐
   │ worker x2      │  通过 etcd 发现 Leader → gRPC 双向流
   │ (协程池 50)    │  scheduler 最少负载选择器分发
   └────────────────┘
```

## 一键部署

```bash
# 0. 前置：一个 K8s 集群（kind / minikube / k3s 均可），kubectl 已配置
#    minikube:  minikube start --cpus=4 --memory=6g
#    kind:      kind create cluster

# 1. 构建镜像（本地 tag，kind/minikube 直接可见）
make k8s-images

# 2. 部署（kustomize 按依赖顺序 apply）
make k8s-apply
# 等价于: kubectl apply -k deployments/k8s

# 3. 等待就绪
kubectl -n dolphin get pods -w
# 期望: etcd-0 / mysql / redis 1/1 Running，scheduler、worker、gateway 各 2/1 Running

# 4. 验证
kubectl -n dolphin get svc
curl http://<node-ip>:30080/health          # gateway 可达
kubectl -n dolphin exec deploy/gateway -- sh -c \
  'wget -qO- http://dolphin-scheduler:9090/healthz'   # scheduler 健康
```

## 使用

```bash
# 在集群内执行管理命令（用 scheduler Service 地址）
kubectl -n dolphin run ctl --rm -it --image=dolphinctl:latest --restart=Never -- \
  --addr dolphin-scheduler:50051 task create --name demo \
  --cron "*/1 * * * *" --handler http://dolphin-gateway:8080/health

# 或本机 port-forward 后直接连
kubectl -n dolphin port-forward svc/dolphin-scheduler 50051:50051 &
./bin/dolphinctl --addr localhost:50051 task list
```

> 注意：本仓库未构建 dolphinctl 镜像。可用 `docker build -t dolphinctl:latest -f deployments/docker/Dockerfile.dolphinctl .`（需先补该 Dockerfile），或直接 `kubectl port-forward` 后用本机二进制。

## 部署后的面试硬数据

部署完成后可直接复跑全部验证脚本（指向集群内地址）：

| 能力 | 验证 | 说明 |
|------|------|------|
| 控制面 HA | `kubectl -n dolphin logs deploy/scheduler` 看 `became leader` 只出现在一个副本 | 2 个 scheduler 副本走 etcd Lease 选主，只有 Leader 跑 Reconciler |
| Worker 负载均衡 | 建 100 个任务后看两个 worker 的 `dolphin_worker_tasks_executing` 分布 | 最少负载选择器按 `current_load/max_concurrency` 比值分发 |
| 执行并发上限 | 触发 N 任务观察 `dolphin_worker_pool_capacity_utilization` 峰值 | 单 worker 上限 = pool.capacity(50)，水平扩 worker 副本即扩容 |
| 限流多实例共享 | 两个 gateway 副本同时打，观察放行数不翻倍 | 共享 Redis 令牌桶（证据 4b） |
| DAG | 跑 `hack/dag_test.ps1` 三连 | 环检测/事件驱动延迟/新鲜度 |
| 故障转移 | `kubectl delete pod <scheduler-leader>` 观察新 Leader 接管 | etcd 租约 15s 内接管 |

## 关键设计决策（面试讲法）

1. **为什么 scheduler 要 2 副本？** 控制面 HA。etcd Lease + Campaign 保证任意时刻只有一个 Leader 运行调度主循环；standby 也提供管理 API（gateway 负载均衡过去即可），Leader 挂了租约到期自动接管。这与 K8s 控制平面（apiserver 多副本 + etcd 共识）的思想一致。

2. **Worker 怎么找到 Leader？** 不硬编码地址。Leader 当选后把自身 gRPC 地址写入 etcd `leader-addr` key（带租约，Leader 死亡自动过期），Worker 监听该 key 自动切换。这就是故障转移的机制根因——和 kubelet 通过 API Server 发现 apiserver 同构。

3. **`advertise_addr` 的坑（真实排障故事）**：最初 scheduler 发布给 Worker 的地址硬编码 `localhost:50051`，单机 docker-compose 没问题（所有进程在同一网络命名空间）。但 K8s 里每个 Pod 网络隔离，Worker 连 `localhost:50051` 指向的是自己。修复：新增 `server.advertise_addr`（或环境变量 `DOLPHIN_ADVERTISE_ADDR`），K8s 清单用 Downward API 注入 `$(POD_IP):50051`。面试讲这个"单机能跑、上集群就挂"的经典问题能证明你真的部署过。

4. **Worker 为什么不需要 Service？** 通信是反向的：Worker 主动连 Leader（etcd 发现），scheduler 通过已建立的 gRPC 双向流下行下发。所以 worker 无 Service，扩缩容直接改 replicas。

5. **Gateway 为什么能多副本限流不翻倍？** 令牌桶在 Redis 原子 Lua 脚本里，多实例共享同一桶（实测放行 1700 vs 独立桶理论 3410，误差 50.1%，证明不翻倍）。

## 生产化清单（面试主动提）

- etcd 单节点 → 3 节点（或托管 etcd），MySQL 加主从 / 用云 RDS。
- 镜像推私有仓库（当前 `make k8s-images` 只打本地 tag）。
- gateway 用 LoadBalancer / Ingress 替代 NodePort；加 TLS。
- 给 worker 配 HPA：按 `dolphin_worker_pool_capacity_utilization` 扩缩容。
- 配置脱敏：JWT secret、MySQL 密码从 ConfigMap 挪到 Secret。
- 健康检查已配 readiness/liveness；可加 PodDisruptionBudget、反亲和。
