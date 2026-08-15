package metrics

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ═══════════════════════════════════════════════
// 网关指标（黄金信号：延迟、流量、错误、饱和度）
// ═══════════════════════════════════════════════

var (
	// 流量：QPS
	GatewayRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dolphin_gateway_requests_total",
			Help: "Total number of HTTP requests processed by gateway.",
		},
		[]string{"method", "path", "status_code"},
	)

	// 延迟：P50/P95/P99
	GatewayRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dolphin_gateway_request_duration_seconds",
			Help:    "HTTP request latency in seconds.",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		},
		[]string{"method", "path"},
	)

	// 错误：非 2xx/3xx
	GatewayErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dolphin_gateway_errors_total",
			Help: "Total number of HTTP errors (5xx) from gateway.",
		},
		[]string{"method", "path", "error_type"},
	)

	// 饱和度：当前正在处理的请求数
	GatewayRequestsInFlight = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "dolphin_gateway_requests_in_flight",
			Help: "Current number of HTTP requests being processed.",
		},
	)

	// 限流拒绝计数
	GatewayRatelimitRejectedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dolphin_gateway_ratelimit_rejected_total",
			Help: "Total number of requests rejected by rate limiter.",
		},
		[]string{"client_id", "endpoint"},
	)

	// 熔断器状态（0=closed, 1=half-open, 2=open）
	GatewayCircuitBreakerState = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dolphin_gateway_circuit_breaker_state",
			Help: "Current state of circuit breaker (0=closed, 1=half-open, 2=open).",
		},
		[]string{"backend"},
	)
)

// ═══════════════════════════════════════════════
// 调度器指标
// ═══════════════════════════════════════════════

var (
	// 任务总数（按状态分）
	SchedulerTasksGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dolphin_scheduler_tasks_total",
			Help: "Current number of tasks by status.",
		},
		[]string{"status"},
	)

	// 调度延迟——核心 SLO 指标
	SchedulerTaskLag = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "dolphin_scheduler_task_lag_seconds",
			Help:    "Time between task's next_run_at and actual dispatch to worker.",
			Buckets: []float64{.1, .25, .5, 1, 2, 5, 10, 30, 60},
		},
	)

	// 调度计数器（仅用 handler_type 聚合，避免 task_id 造成 label 基数爆炸）
	SchedulerDispatchTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dolphin_scheduler_dispatch_total",
			Help: "Total number of tasks dispatched to workers.",
		},
		[]string{"handler_type", "result"},
	)

	// Leader 选举次数
	SchedulerLeaderElections = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "dolphin_scheduler_leader_elections_total",
			Help: "Total number of leader elections that occurred.",
		},
	)

	// 当前节点是否为 Leader
	SchedulerIsLeader = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "dolphin_scheduler_is_leader",
			Help: "1 if this instance is the current leader, 0 otherwise.",
		},
	)

	// 当前实例内存注册表中持有活跃流的在线 worker 数。
	// 与 MySQL workers 表不同：这个值反映"本实例真正能下发任务的 worker"。
	// 故障转移测试用它判断 Worker 是否已重连到新 Leader（MySQL online 只是残留状态）。
	SchedulerWorkersOnline = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "dolphin_scheduler_workers_online",
			Help: "Number of workers with an active gRPC stream registered in this instance's in-memory registry.",
		},
	)

	// WorkQueue 深度
	SchedulerQueueDepth = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "dolphin_scheduler_queue_depth",
			Help: "Current number of items pending in the work queue.",
		},
	)

	// 下发耗时：Reconciler 调 executor.Execute → stream.Send 到 worker 的耗时。
	// 若 Send 因 gRPC 流控/worker 读得慢而阻塞，这里会显示高延迟（毫秒级甚至秒级），
	// 直接暴露"下发管道被串行化"这一瓶颈（场景 C 排查用）。
	SchedulerDispatchLatency = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "dolphin_scheduler_dispatch_latency_seconds",
			Help:    "Time spent sending one task dispatch to a worker (stream.Send).",
			Buckets: []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		},
	)

	// Reconcile 耗时
	SchedulerReconcileDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "dolphin_scheduler_reconcile_duration_seconds",
			Help:    "Time taken for one reconcile cycle.",
			Buckets: []float64{.001, .005, .01, .05, .1, .5, 1, 5},
		},
	)

	// 漏调度计数
	SchedulerMissedSchedules = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "dolphin_scheduler_missed_schedules_total",
			Help: "Total number of schedules that were missed.",
		},
	)

	// 故障转移恢复延迟
	SchedulerFailoverRecoveryTime = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "dolphin_failover_recovery_seconds",
			Help:    "Time from worker death detection to task re-assignment.",
			Buckets: []float64{1, 5, 10, 15, 30, 60, 120},
		},
	)

	// DAG 依赖阻塞的任务数（当前挂起等待上游）
	SchedulerDagBlocked = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "dolphin_scheduler_dag_blocked_tasks",
			Help: "Current number of tasks blocked on unmet dependencies.",
		},
	)

	// 依赖门控次数：任务到期但因依赖未满足被挂起
	SchedulerDagGateTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "dolphin_scheduler_dag_gate_total",
			Help: "Total number of times a due task was held because dependencies were unmet.",
		},
	)

	// DAG 环检测拒绝次数（创建/更新任务时）
	SchedulerDagCycleRejectTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "dolphin_scheduler_dag_cycle_reject_total",
			Help: "Total number of DAG create/update attempts rejected due to a cycle.",
		},
	)

	// 执行级重试计数（result: scheduled/dispatched/exhausted/dispatch_failed）
	SchedulerRetryTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dolphin_scheduler_retry_total",
			Help: "Total number of execution retry events by result.",
		},
		[]string{"result"},
	)

	// 当前排队/执行中的重试数（in-memory，Leader 崩溃后归零，cron 兜底）
	SchedulerRetriesPending = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "dolphin_scheduler_retries_pending",
			Help: "Current number of retries scheduled (in-memory).",
		},
	)

	// stale running 兜底救援次数：running 日志超过 task.timeout+grace 被判定为结果丢失
	SchedulerStaleRunningRescuedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "dolphin_scheduler_stale_running_rescued_total",
			Help: "Total number of running task logs rescued because their result was lost.",
		},
	)
)

// ═══════════════════════════════════════════════
// Worker 指标
// ═══════════════════════════════════════════════

var (
	// 当前正在执行的任务数
	WorkerTasksExecuting = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "dolphin_worker_tasks_executing",
			Help: "Current number of tasks being executed by this worker.",
		},
	)

	// 任务执行耗时
	WorkerTaskDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dolphin_worker_task_duration_seconds",
			Help:    "Task execution duration in seconds.",
			Buckets: []float64{.01, .05, .1, .5, 1, 5, 10, 30, 60, 300},
		},
		[]string{"handler_type"},
	)

	// 任务完成计数
	WorkerTaskCompleted = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dolphin_worker_task_completed_total",
			Help: "Total number of task executions completed.",
		},
		[]string{"status"}, // success/failed/timeout/cancelled
	)

	// 池满拒绝计数：Submit 因队列满被拒（主动背压）的次数。
	// 场景 C（池满自动重试）用它直测背压是否真实发生：
	// 调度器把这些任务标 failed → 自动重试 → 新实例执行成功。
	WorkerPoolRejectedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "dolphin_worker_pool_rejected_total",
			Help: "Total number of dispatches rejected because the worker pool was full (active backpressure).",
		},
	)

	// Worker 协程池利用率
	WorkerPoolUtilization = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "dolphin_worker_pool_capacity_utilization",
			Help: "Ratio of current_load to max_concurrency (0.0 - 1.0).",
		},
	)

	// Worker 池在途数：current_load = 正在执行 + 排队等待的任务数。
	// 配合 dolphin_worker_tasks_executing 可推出排队数 = inflight - executing：
	//   - inflight 高（接近 150 上限）而 executing 低 → 任务堵在 worker 队列（下发是突发、worker 消化慢）
	//   - inflight ≈ executing → worker 队列空（下发是涓流，瓶颈在调度器侧）
	// 场景 C 排查"池满背压为何不触发"的关键判据。
	WorkerPoolInflight = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "dolphin_worker_pool_inflight",
			Help: "Current number of tasks queued or executing in the worker pool (current_load).",
		},
	)

	// Worker 心跳延迟
	WorkerHeartbeatLatency = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "dolphin_worker_heartbeat_latency_seconds",
			Help:    "Round-trip time for heartbeat messages.",
			Buckets: []float64{.01, .05, .1, .5, 1, 2},
		},
	)

	// Go routine 数量
	WorkerGoroutines = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "dolphin_worker_goroutines",
			Help: "Current number of goroutines in this worker process.",
		},
	)
)

// ═══════════════════════════════════════════════
// 辅助记录函数
// ═══════════════════════════════════════════════

// RecordAPIMetrics 网关中间件调用的便捷函数。
func RecordAPIMetrics(method, path string, statusCode int, duration time.Duration) {
	status := fmt.Sprintf("%d", statusCode)
	GatewayRequestsTotal.WithLabelValues(method, path, status).Inc()
	GatewayRequestDuration.WithLabelValues(method, path).Observe(duration.Seconds())
	if statusCode >= 500 {
		GatewayErrorsTotal.WithLabelValues(method, path, "5xx").Inc()
	}
}

// RecordDispatch 调度器下发任务时记录调度计数和调度延迟。
// lag = 任务到期(next_run_at)到实际下发的耗时。
func RecordDispatch(handlerType string, lag time.Duration, success bool) {
	result := "success"
	if !success {
		result = "failed"
	}
	SchedulerDispatchTotal.WithLabelValues(handlerType, result).Inc()
	SchedulerTaskLag.Observe(lag.Seconds())
}

// RecordDispatchLatency 记录一次下发到 worker 的耗时（stream.Send）。
// 高值说明 gRPC 下发管道被流控/慢读取阻塞，是场景 C 排查的关键信号。
func RecordDispatchLatency(d time.Duration) {
	SchedulerDispatchLatency.Observe(d.Seconds())
}

// SetQueueDepth 更新调度 WorkQueue 中待处理的 key 数。
func SetQueueDepth(n int) {
	SchedulerQueueDepth.Set(float64(n))
}

// SetWorkerPoolInflight 更新 worker 池在途任务数（排队 + 执行）。
func SetWorkerPoolInflight(n int64) {
	WorkerPoolInflight.Set(float64(n))
}

// RecordReconcile reconcile 循环耗时。
func RecordReconcile(d time.Duration) {
	SchedulerReconcileDuration.Observe(d.Seconds())
}

// RecordMissedSchedule 漏调度计数。
func RecordMissedSchedule() {
	SchedulerMissedSchedules.Inc()
}

// RecordDagGate 记录一次依赖门控（到期任务因依赖未满足被挂起）。
func RecordDagGate() {
	SchedulerDagGateTotal.Inc()
}

// RecordDagCycleReject 记录一次 DAG 环检测拒绝。
func RecordDagCycleReject() {
	SchedulerDagCycleRejectTotal.Inc()
}

// RecordRetryScheduled 记录一次重试排队。
func RecordRetryScheduled() {
	SchedulerRetryTotal.WithLabelValues("scheduled").Inc()
}

// RecordRetryDispatched 记录一次重试实际下发。
func RecordRetryDispatched() {
	SchedulerRetryTotal.WithLabelValues("dispatched").Inc()
}

// RecordRetryExhausted 记录一次重试耗尽（最终失败）。
func RecordRetryExhausted() {
	SchedulerRetryTotal.WithLabelValues("exhausted").Inc()
}

// RecordRetryDispatchFailed 记录重试下发失败（worker 不可用）。
func RecordRetryDispatchFailed() {
	SchedulerRetryTotal.WithLabelValues("dispatch_failed").Inc()
}

// SetRetriesPending 设置当前排队/执行中的重试数。
func SetRetriesPending(n int64) {
	SchedulerRetriesPending.Set(float64(n))
}

// RecordStaleRunningRescued 记录一次 stale running 兜底救援。
func RecordStaleRunningRescued() {
	SchedulerStaleRunningRescuedTotal.Inc()
}

// RecordFailoverRecovery 记录故障转移恢复耗时。
// 从 Worker 心跳超时检测到任务重新下发的时间。
func RecordFailoverRecovery(d time.Duration) {
	SchedulerFailoverRecoveryTime.Observe(d.Seconds())
}

// RecordWorkerTaskStarted worker 开始执行任务。
func RecordWorkerTaskStarted() {
	WorkerTasksExecuting.Inc()
	// 池利用率由调用方设置，这里只维护 executing 计数。
}

// RecordWorkerTaskCompleted worker 完成任务，记录耗时和状态。
func RecordWorkerTaskCompleted(handlerType string, duration time.Duration, status string) {
	WorkerTasksExecuting.Dec()
	WorkerTaskDuration.WithLabelValues(handlerType).Observe(duration.Seconds())
	WorkerTaskCompleted.WithLabelValues(status).Inc()
}

// RecordPoolRejected worker 协程池满、拒绝一次下发（主动背压）。
func RecordPoolRejected() {
	WorkerPoolRejectedTotal.Inc()
}
