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

	// Worker 协程池利用率
	WorkerPoolUtilization = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "dolphin_worker_pool_capacity_utilization",
			Help: "Ratio of current_load to max_concurrency (0.0 - 1.0).",
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

// RecordReconcile reconcile 循环耗时。
func RecordReconcile(d time.Duration) {
	SchedulerReconcileDuration.Observe(d.Seconds())
}

// RecordMissedSchedule 漏调度计数。
func RecordMissedSchedule() {
	SchedulerMissedSchedules.Inc()
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
