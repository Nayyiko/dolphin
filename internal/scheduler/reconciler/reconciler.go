package reconciler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/yourname/dolphin/internal/pkg/metrics"
	"github.com/yourname/dolphin/internal/pkg/model"
	"github.com/yourname/dolphin/internal/scheduler/dag"
	"github.com/yourname/dolphin/internal/scheduler/informer"
	"github.com/yourname/dolphin/internal/scheduler/manager"
	"github.com/yourname/dolphin/internal/scheduler/queue"
)

// 执行级重试的默认参数。
const (
	// defaultRetryBaseDelay 重试基础退避：attempt n 的延迟 = base * 2^(n-1)。
	defaultRetryBaseDelay = 2 * time.Second
	// defaultStaleRunningGrace running 日志超过 task.timeout + grace 视为结果丢失。
	defaultStaleRunningGrace = 30 * time.Second
	// maxRetryDispatchFailures 单次重试尝试里，因 worker 不可用而重新下发的次数上限。
	maxRetryDispatchFailures = 5
)

// DolphinFinalizer 终结器名称。任务删除时先执行清理，再移除终结器真正删除。
const DolphinFinalizer = "dolphin.io/task-cleanup"

// TaskExecutor 任务执行接口。
// 由 server 层实现（将任务下发给 Worker）。
type TaskExecutor interface {
	// Execute 使用给定的 instanceID 将任务下发给 Worker。返回执行结果。
	Execute(ctx context.Context, task *model.Task, instanceID string) (ExecuteResult, error)
	// Cancel 通知 Worker 取消某次执行。
	Cancel(ctx context.Context, instanceID string) error
}

// ExecuteResult 执行结果。
type ExecuteResult struct {
	WorkerID   string
	InstanceID string
}

// Reconciler 任务协调器。
// 对标 Kubernetes Reconciler 接口：
//   每个控制周期检查"期望状态 vs 实际状态"，持续纠正偏差。
// 调度逻辑：
//   1. Informer 事件 → EnqueueTask(taskID)
//   2. WorkQueue.Get → 并行 reconcile
//   3. 到期任务 → 选 Worker 下发 → 更新 next_run_at + Conditions
//   4. 失败 → AddRateLimited（指数退避重试）
type Reconciler struct {
	queue    *queue.WorkQueue
	lister   informer.TaskLister
	db       *gorm.DB
	manager  *manager.Manager
	executor TaskExecutor
	cronNext func(expr string) (time.Time, error)

	workers int

	mu      sync.Mutex
	running map[string]bool // taskID → 正在执行中（避免重复调度）
	blocked map[string]bool // taskID → 依赖未满足被挂起（用于 DAG 指标）

	// 执行级重试状态。
	retryBaseDelay time.Duration
	leaderMu       sync.RWMutex
	leaderCtx      context.Context // 当前任期的 Leader ctx（失权后 cancel，重试停止）
	retryMu        sync.Mutex
	retrying       map[string]bool // taskID → 已有重试在排队/执行中（防重复）
	dispatchFailures map[string]int // taskID → 同一次重试尝试下发失败次数
	retryPending   atomic.Int64    // 排队中的重试数（指标）
}

// NewReconciler 创建协调器。
func NewReconciler(
	q *queue.WorkQueue,
	lister informer.TaskLister,
	mgr *manager.Manager,
	executor TaskExecutor,
	cronNext func(expr string) (time.Time, error),
	numWorkers int,
) *Reconciler {
	if numWorkers <= 0 {
		numWorkers = 4
	}
	return &Reconciler{
		queue:          q,
		lister:         lister,
		db:             mgr.DB(),
		manager:        mgr,
		executor:       executor,
		cronNext:       cronNext,
		workers:        numWorkers,
		running:        make(map[string]bool),
		blocked:        make(map[string]bool),
		retryBaseDelay: defaultRetryBaseDelay,
		retrying:       make(map[string]bool),
		dispatchFailures: make(map[string]int),
	}
}

// SetLeaderCtx 设置当前任期的 Leader context。
// 由 main 在 become leader 时注入；失权后该 ctx 被 cancel，重试定时器随之停止。
func (r *Reconciler) SetLeaderCtx(ctx context.Context) {
	r.leaderMu.Lock()
	defer r.leaderMu.Unlock()
	r.leaderCtx = ctx
}

// SetRetryPolicy 设置重试策略参数（<=0 时保持默认）。
func (r *Reconciler) SetRetryPolicy(baseDelay time.Duration) {
	if baseDelay <= 0 {
		return
	}
	r.retryBaseDelay = baseDelay
}

func (r *Reconciler) getLeaderCtx() context.Context {
	r.leaderMu.RLock()
	defer r.leaderMu.RUnlock()
	return r.leaderCtx
}

// EnqueueTask 将任务推入协调队列（由 Informer 事件处理器调用）。
func (r *Reconciler) EnqueueTask(taskID string) {
	r.queue.Add(taskID)
}

// EnqueueAfter 延迟入队（预计算 cron 到期时间）。
func (r *Reconciler) EnqueueAfter(taskID string, delay time.Duration) {
	r.queue.AddAfter(taskID, delay)
}

// EnqueueDependents 上游任务完成时，推送直接依赖它的下游任务重新检查依赖。
// 由 server 层在收到 TaskResult 后调用（事件驱动 DAG，毫秒级唤醒下游）。
// 不校验上游结果状态——下游 reconcile 时会按自己的 DepPolicy 重新判定，
// 失败的上游对 all_success 下游仍然保持阻塞。
func (r *Reconciler) EnqueueDependents(upstreamID string) {
	for _, item := range r.lister.List() {
		t := item.Task
		if t.Status != model.TaskStatusActive {
			continue
		}
		deps, err := dag.ParseDependOn(t.DependOn)
		if err != nil {
			continue
		}
		for _, up := range deps {
			if up == upstreamID {
				r.EnqueueTask(t.ID)
				break
			}
		}
	}
}

// Run 启动多个并行 reconciler worker。
func (r *Reconciler) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < r.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.worker(ctx)
		}()
	}
	wg.Wait()
}

// RunDueTaskScanner 定期扫描到期任务并入队。
// cron 调度自动触发的核心机制：扫描 next_run_at <= now 的任务，推入 WorkQueue。
// 仅在 Leader 节点运行，interval 建议 1s。
func (r *Reconciler) RunDueTaskScanner(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tasks, err := r.manager.ListDueTasks(ctx, time.Now())
			if err != nil {
				slog.Warn("due task scan failed", "err", err)
				continue
			}
			for i := range tasks {
				r.EnqueueTask(tasks[i].ID)
			}
			if len(tasks) > 0 {
				slog.Debug("due task scanner enqueued", "count", len(tasks))
			}
		}
	}
}

// RunWorkerMonitor 定期检测心跳超时的 Worker，触发故障转移。
// 仅 Leader 运行。心跳超时后:
//  1. 标记 Worker offline（DB + 内存 registry）
//  2. 释放该 Worker 上所有 running 的 TaskLog（置为 failed，带错误信息）
//  3. 将相关任务的 next_run_at 置为 now，立即重新调度到其他 Worker
//
// 这实现了"任务不丢不重"：Worker 挂了任务自动转移，且不重复执行。
func (r *Reconciler) RunWorkerMonitor(ctx context.Context, heartbeatTimeout time.Duration, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.checkWorkerHeartbeats(ctx, heartbeatTimeout); err != nil {
				slog.Warn("worker heartbeat check failed", "err", err)
			}
		}
	}
}

// checkWorkerHeartbeats 检测心跳超时并执行故障转移。
func (r *Reconciler) checkWorkerHeartbeats(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(-timeout)

	var stale []model.Worker
	if err := r.db.WithContext(ctx).
		Where("status = ? AND last_heartbeat < ?", model.WorkerStatusOnline, deadline).
		Find(&stale).Error; err != nil {
		return err
	}
	if len(stale) == 0 {
		return nil
	}

	for i := range stale {
		w := stale[i]
		slog.Warn("worker heartbeat timeout, failing over",
			"worker_id", w.ID, "last_heartbeat", w.LastHeartbeat, "timeout", timeout)

		// 1. 标记离线
		if err := r.db.WithContext(ctx).Model(&model.Worker{}).
			Where("id = ?", w.ID).Update("status", model.WorkerStatusOffline).Error; err != nil {
			slog.Warn("mark worker offline failed", "worker_id", w.ID, "err", err)
			continue
		}

		// 2. 释放该 worker 上所有 running 任务（置 failed）
		now := time.Now()
		var runningLogs []model.TaskLog
		if err := r.db.WithContext(ctx).
			Where("worker_id = ? AND status = ?", w.ID, model.TaskLogStatusRunning).
			Find(&runningLogs).Error; err != nil {
			slog.Warn("query running logs failed", "worker_id", w.ID, "err", err)
			continue
		}

		failedTaskIDs := make([]string, 0, len(runningLogs))
		for j := range runningLogs {
			l := runningLogs[j]
			errMsg := "worker heartbeat timeout, task re-scheduled"
			if err := r.db.WithContext(ctx).Model(&l).Updates(map[string]any{
				"status":    model.TaskLogStatusFailed,
				"end_time":  &now,
				"error_msg": errMsg,
			}).Error; err != nil {
				slog.Warn("fail running log failed", "instance_id", l.InstanceID, "err", err)
				continue
			}
			failedTaskIDs = append(failedTaskIDs, l.TaskID)
		}
		slog.Info("worker failover: released running tasks",
			"worker_id", w.ID, "released", len(failedTaskIDs))

		// 3. 将受影响任务的 next_run_at 置为 now，触发立即重新调度
		//    去重，避免同一任务多次更新
		seen := make(map[string]bool)
		for _, tid := range failedTaskIDs {
			if seen[tid] {
				continue
			}
			seen[tid] = true
			if err := r.manager.UpdateNextRunAt(ctx, tid, time.Now()); err != nil {
				slog.Warn("reset next_run_at failed", "task_id", tid, "err", err)
				continue
			}
			// 同步更新本地缓存，让 scanner/事件处理立即看到
			r.lister.UpdateNextRunAt(tid, time.Now())
			// 直接入队，立即重新调度
			r.EnqueueTask(tid)
		}

			slog.Info("worker failover complete", "worker_id", w.ID, "tasks_requeued", len(seen))
	}
	return nil
}

// RunStaleRunningScanner 兜底救援：定期扫描 running 日志，
// 若其存活时间超过 task.timeout + grace（结果上报大概率丢失/worker 挂死），
// 置为 failed 并触发重试。这是执行结果链路可靠性的最后一道保险。
// 仅 Leader 运行。
func (r *Reconciler) RunStaleRunningScanner(ctx context.Context, interval, grace time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.rescueStaleRunning(ctx, grace); err != nil {
				slog.Warn("stale running scan failed", "err", err)
			}
		}
	}
}

// rescueStaleRunning 扫描并救援超龄 running 日志。
func (r *Reconciler) rescueStaleRunning(ctx context.Context, grace time.Duration) error {
	if grace <= 0 {
		grace = defaultStaleRunningGrace
	}
	now := time.Now()

	var logs []model.TaskLog
	if err := r.db.WithContext(ctx).
		Where("status = ?", model.TaskLogStatusRunning).
		Find(&logs).Error; err != nil {
		return err
	}

	for i := range logs {
		l := logs[i]
		// 该任务允许的最长执行窗口：task.timeout + grace
		timeout := 30
		if item, ok := r.lister.Get(l.TaskID); ok && item.Task.Timeout > 0 {
			timeout = item.Task.Timeout
		}
		deadline := l.StartTime.Add(time.Duration(timeout)*time.Second + grace)
		if now.Before(deadline) {
			continue
		}

		metrics.RecordStaleRunningRescued()
		errMsg := "stale running: result report lost or worker hung, re-scheduling"
		if err := r.db.WithContext(ctx).Model(&l).Updates(map[string]any{
			"status":    model.TaskLogStatusFailed,
			"end_time":  &now,
			"error_msg": errMsg,
		}).Error; err != nil {
			slog.Warn("rescue stale running failed", "instance_id", l.InstanceID, "err", err)
			continue
		}
		slog.Warn("rescued stale running task", "task_id", l.TaskID,
			"instance_id", l.InstanceID, "retry_count", l.RetryCount,
			"timeout", timeout, "grace", grace.Round(time.Second))
		// 触发重试（失败结果走同一套重试逻辑）
		r.scheduleRetry(l.TaskID)
	}
	return nil
}

func (r *Reconciler) worker(ctx context.Context) {
	for {
		key, shutdown := r.queue.GetCtx(ctx)
		if shutdown {
			return
		}
		start := time.Now()
		err := r.reconcile(ctx, key)
		metrics.RecordReconcile(time.Since(start))
		if err != nil {
			// 失败 → 限速重新入队（指数退避）
			slog.Warn("reconcile failed, rate-limited requeue", "task_id", key, "err", err)
			r.queue.AddRateLimited(key)
		} else {
			// 成功 → 清除限速记录
			r.queue.Forget(key)
			r.queue.Done(key)
		}
	}
}

// reconcile 对单个任务执行一次协调。
func (r *Reconciler) reconcile(ctx context.Context, taskID string) error {
	// 1. 从本地缓存获取任务（不走数据库）
	item, ok := r.lister.Get(taskID)
	if !ok {
		// 缓存中不存在（可能刚删除），忽略
		return nil
	}
	task := item.Task

	// 2. 处理删除（Finalizer 模式）
	if task.DeletionTimestamp != nil {
		return r.handleDeletion(ctx, &task)
	}

	// 3. 非 active 任务跳过
	if task.Status != model.TaskStatusActive {
		return nil
	}

	// 4. 判断是否需要调度
	need, reason := r.needExecution(ctx, &task)
	if need {
		// 4.1 DAG 依赖门控：依赖未满足则挂起。
		// 保持 next_run_at 不变（仍处于 due），由到期扫描器（1s）和上游完成事件
		// 共同兜底唤醒；不会错误推进调度周期，也不会重复下发。
		satisfied, depReason, hasDeps := r.depsSatisfied(ctx, &task)
		if hasDeps {
			if !satisfied {
				r.setBlocked(task.ID, true)
				metrics.RecordDagGate()
				r.setCondition(ctx, task.ID, model.ConditionDeps, model.StatusFalse,
					"BlockedOnDeps", depReason)
				slog.Debug("task blocked on dependencies", "task_id", task.ID, "reason", depReason)
				return nil
			}
			r.setBlocked(task.ID, false)
			r.setCondition(ctx, task.ID, model.ConditionDeps, model.StatusTrue,
				"DepsSatisfied", "")
		}
		if err := r.dispatch(ctx, &task, 0, true); err != nil {
			return err
		}
		slog.Debug("task dispatched", "task_id", task.ID, "reason", reason)
	}

	// 5. 补偿漏调度
	r.handleMissedSchedule(ctx, &task)

	return nil
}

// needExecution 判断任务是否应在本周期触发。
func (r *Reconciler) needExecution(ctx context.Context, task *model.Task) (bool, string) {
	now := time.Now()

	// 已到期
	if !task.NextRunAt.After(now) {
		// 避免重复调度：检查是否有运行中的实例
		if r.hasRunningInstance(ctx, task.ID) {
			return false, "already-running"
		}
		return true, "due"
	}
	return false, "not-due"
}

// hasRunningInstance 检查任务是否有未完成的执行实例。
func (r *Reconciler) hasRunningInstance(ctx context.Context, taskID string) bool {
	var count int64
	r.db.WithContext(ctx).Model(&model.TaskLog{}).
		Where("task_id = ? AND status = ?", taskID, model.TaskLogStatusRunning).
		Count(&count)
	return count > 0
}

// depsSatisfied 判断任务依赖是否满足，返回 (是否满足, 原因, 是否有依赖)。
// 语义（新鲜度）：每个上游必须在"本任务上次运行之后"有符合策略的执行，
// 保证依赖不会串到上一次运行的旧结果。
func (r *Reconciler) depsSatisfied(ctx context.Context, task *model.Task) (bool, string, bool) {
	deps, err := dag.ParseDependOn(task.DependOn)
	if err != nil || len(deps) == 0 {
		return true, "", false
	}
	policy := task.DepPolicy
	if policy == "" {
		policy = model.DepPolicyAllSuccess
	}
	lastRun := r.lastRunTime(ctx, task.ID)
	lookup := func(upID string, after time.Time) *model.TaskLog {
		return r.latestExecutionAfter(ctx, upID, after)
	}
	ok, reason := dag.DepsSatisfied(deps, policy, lastRun, lookup)
	return ok, reason, true
}

// lastRunTime 返回任务最近一次执行的开始时间；从未运行返回零值。
func (r *Reconciler) lastRunTime(ctx context.Context, taskID string) time.Time {
	var logs []model.TaskLog
	r.db.WithContext(ctx).
		Where("task_id = ?", taskID).
		Order("start_time DESC").
		Limit(1).
		Find(&logs)
	if len(logs) == 0 {
		return time.Time{}
	}
	return logs[0].StartTime
}

// latestExecutionAfter 返回 taskID 在 after 之后最近一次执行（任意状态）；无则 nil。
func (r *Reconciler) latestExecutionAfter(ctx context.Context, taskID string, after time.Time) *model.TaskLog {
	var logs []model.TaskLog
	r.db.WithContext(ctx).
		Where("task_id = ? AND start_time > ?", taskID, after).
		Order("start_time DESC").
		Limit(1).
		Find(&logs)
	if len(logs) == 0 {
		return nil
	}
	return &logs[0]
}

// setBlocked 维护当前被依赖阻塞的任务集合，并同步 gauge 指标。
func (r *Reconciler) setBlocked(taskID string, blocked bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, exists := r.blocked[taskID]
	if blocked && !exists {
		r.blocked[taskID] = true
		metrics.SchedulerDagBlocked.Set(float64(len(r.blocked)))
	} else if !blocked && exists {
		delete(r.blocked, taskID)
		metrics.SchedulerDagBlocked.Set(float64(len(r.blocked)))
	}
}

// dispatch 选择 Worker 并下发任务。
//
// retryCount 是本次尝试的编号（0=首次执行，1..N=第 N 次重试），写入 task_log。
// advanceNext 为 true 时（首次下发）预计算并推进 next_run_at；重试时传 false，
// 因为首次下发已推进过，重试不应把 cron 周期再往前推。
func (r *Reconciler) dispatch(ctx context.Context, task *model.Task, retryCount int, advanceNext bool) error {
	// 防止并发重复调度
	r.mu.Lock()
	if r.running[task.ID] {
		r.mu.Unlock()
		return nil
	}
	r.running[task.ID] = true
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.running, task.ID)
		r.mu.Unlock()
	}()

	// 先落执行日志，再下发 Worker。
	// 顺序保证：Worker 的快速拒绝/结果上报到达时，日志必然已存在，
	// 否则会出现"结果先到、log 未建"→ 更新 0 行 + 幻影 running 日志的竞态。
	instanceID := uuid.NewString()
	now := time.Now()
	log := &model.TaskLog{
		ID:         uuid.NewString(),
		TaskID:     task.ID,
		InstanceID: instanceID,
		Status:     model.TaskLogStatusRunning,
		StartTime:  now,
		RetryCount: retryCount,
		CreatedAt:  now,
	}
	if err := r.db.WithContext(ctx).Create(log).Error; err != nil {
		return err
	}

	// 执行（下发 Worker）
	result, err := r.executor.Execute(ctx, task, instanceID)
	if err != nil {
		// 下发失败（无可用 Worker/断连）：把日志标记失败，交给上层
		// （reconcile 限速重入队 / doRetry 退避）重新调度。
		r.db.WithContext(ctx).Model(log).Updates(map[string]any{
			"status":    model.TaskLogStatusFailed,
			"end_time":  &now,
			"error_msg": err.Error(),
		})
		return err
	}

	// 回填 Worker（下发达成后才有 worker_id；空值会导致 Worker 故障转移
	// 无法匹配该 running 日志，故必须补齐）。
	if result.WorkerID != "" {
		r.db.WithContext(ctx).Model(log).Update("worker_id", result.WorkerID)
	}

	// 记录调度指标：计数 + 调度延迟（任务到期时间 → 实际下发时间）
	lag := time.Since(task.NextRunAt)
	if lag < 0 {
		lag = 0 // 手动触发/重试时 next_run_at 在未来，取 0
	}
	metrics.RecordDispatch(task.HandlerType, lag, true)

	// 预计算下一次触发时间（仅首次下发推进 cron 周期）
	if advanceNext && r.cronNext != nil {
		next, err := r.cronNext(task.CronExpr)
		if err == nil {
			if err := r.manager.UpdateNextRunAt(ctx, task.ID, next); err != nil {
				return err
			}
			// 同步更新本地缓存：避免 scanner 在 Informer 下一轮刷新前
			// 仍看到旧的 next_run_at，导致同一 cron 周期重复调度。
			r.lister.UpdateNextRunAt(task.ID, next)
		}
	}

	// 更新 Conditions
	condReason := "Dispatched"
	if retryCount > 0 {
		condReason = "Retrying"
	}
	r.setCondition(ctx, task.ID, model.ConditionScheduled, model.StatusTrue,
		condReason, fmt.Sprintf("dispatched attempt %d to worker %s", retryCount, result.WorkerID))
	r.setCondition(ctx, task.ID, model.ConditionRunning, model.StatusTrue,
		"Executing", "instance "+instanceID)

	return nil
}

// OnTaskResult 任务执行结果到达时的回调（由 server 层在收到 TaskResult 后调用）。
// 做两件事：
//  1. 事件驱动 DAG：推送直接依赖该任务的下游重新检查依赖。
//  2. 执行级重试：失败/超时且重试次数未耗尽 → 按指数退避自动重新下发。
func (r *Reconciler) OnTaskResult(taskID, status string) {
	r.EnqueueDependents(taskID)
	if status == model.TaskLogStatusFailed || status == model.TaskLogStatusTimeout {
		r.scheduleRetry(taskID)
	}
}

// retryBackoff 返回第 n 次重试的退避时长（n 从 0 起）：base * 2^n。
func (r *Reconciler) retryBackoff(n int) time.Duration {
	base := r.retryBaseDelay
	if base <= 0 {
		base = defaultRetryBaseDelay
	}
	if n > 30 {
		n = 30 // 防止整数溢出
	}
	return base * time.Duration(1<<uint(n))
}

// retryDecision 纯函数：根据已发生的重试次数和上限，决定下一次重试的
// (attempt 编号, 退避时长, 是否可重试)。
// lastRetryCount 是最近一次尝试的编号（0=首次执行），maxRetries 是上限。
func retryDecision(lastRetryCount, maxRetries int, baseDelay time.Duration) (attempt int, delay time.Duration, ok bool) {
	if maxRetries <= 0 {
		return 0, 0, false // 重试被禁用
	}
	attempt = lastRetryCount + 1
	if attempt > maxRetries {
		return 0, 0, false // 已耗尽
	}
	if baseDelay <= 0 {
		baseDelay = defaultRetryBaseDelay
	}
	n := attempt - 1
	if n > 30 {
		n = 30
	}
	return attempt, baseDelay * time.Duration(1<<uint(n)), true
}

// lastRetryCount 返回任务最近一次尝试的重试编号（0=首次，n=第 n 次重试）。
func (r *Reconciler) lastRetryCount(taskID string) int {
	var logs []model.TaskLog
	r.db.Where("task_id = ?", taskID).
		Order("created_at DESC").
		Limit(1).
		Find(&logs)
	if len(logs) == 0 {
		return 0
	}
	return logs[0].RetryCount
}

// scheduleRetry 为失败的任务安排一次重试。
// 幂等：同一任务同时只有一个重试在排队/执行中（retrying 标记去重）。
// 重试状态在内存中（Leader 崩溃后丢失，由 cron 兜底保证最终会再执行）。
func (r *Reconciler) scheduleRetry(taskID string) {
	ctx := r.getLeaderCtx()
	if ctx == nil || ctx.Err() != nil {
		return // 非 Leader / 已失权：不安排重试
	}

	r.retryMu.Lock()
	if r.retrying[taskID] {
		r.retryMu.Unlock()
		return
	}
	r.retrying[taskID] = true
	r.retryMu.Unlock()

	// 任务不存在/非 active/显式 max_retries=0 → 不重试
	task, err := r.manager.Get(ctx, taskID)
	if err != nil || task.Status != model.TaskStatusActive || task.MaxRetries <= 0 {
		r.clearRetrying(taskID)
		return
	}

	// 纯函数决策：已尝试次数 → 下一次 (attempt, delay)。耗尽则最终失败。
	attempt, delay, ok := retryDecision(r.lastRetryCount(taskID), int(task.MaxRetries), r.retryBaseDelay)
	if !ok {
		// maxRetries=0 已在上面排除，这里只能是"已耗尽"。
		r.setCondition(ctx, taskID, model.ConditionRetries, model.StatusFalse,
			"RetriesExhausted",
			fmt.Sprintf("task failed after %d attempts (max_retries=%d)", int(task.MaxRetries)+1, task.MaxRetries))
		metrics.RecordRetryExhausted()
		r.clearRetrying(taskID)
		return
	}

	// 已有执行中的实例（如手动触发抢先）→ 等它出结果自行触发重试逻辑
	if r.hasRunningInstance(ctx, taskID) {
		r.clearRetrying(taskID)
		return
	}

	metrics.RecordRetryScheduled()
	r.retryPending.Add(1)
	metrics.SetRetriesPending(r.retryPending.Load())
	slog.Info("execution retry scheduled",
		"task_id", taskID, "attempt", attempt, "delay", delay.Round(time.Millisecond))

	time.AfterFunc(delay, func() {
		r.doRetry(ctx, taskID, attempt)
	})
}

// doRetry 执行一次重试下发。
func (r *Reconciler) doRetry(ctx context.Context, taskID string, attempt int) {
	r.retryPending.Add(-1)
	metrics.SetRetriesPending(r.retryPending.Load())

	if ctx.Err() != nil {
		r.clearRetrying(taskID)
		return
	}

	// 以 DB 为准重新读取任务（缓存可能滞后于 pause/删除）
	task, err := r.manager.Get(ctx, taskID)
	if err != nil || task.Status != model.TaskStatusActive || attempt > task.MaxRetries {
		r.clearRetrying(taskID)
		return
	}

	// 已存在执行中的实例（手动触发等）→ 跳过，避免重复执行
	if r.hasRunningInstance(ctx, taskID) {
		r.clearRetrying(taskID)
		return
	}

	metrics.RecordRetryDispatched()
	if err := r.dispatch(ctx, task, attempt, false); err != nil {
		// 下发失败（worker 暂时不可用）→ 保持 retrying 标记，退避后重试同一次尝试。
		// 有次数上限，避免 worker 一直不来时无限自旋。
		metrics.RecordRetryDispatchFailed()
		failures := r.retryDispatchFailures(taskID)
		if failures >= maxRetryDispatchFailures {
			slog.Warn("retry dispatch giving up after repeated failures", "task_id", taskID,
				"attempt", attempt, "failures", failures)
			r.clearRetrying(taskID)
			return
		}
		delay := r.retryBackoff(attempt - 1)
		slog.Warn("retry dispatch failed, retrying later", "task_id", taskID,
			"attempt", attempt, "err", err, "delay", delay.Round(time.Millisecond))
		time.AfterFunc(delay, func() {
			r.doRetry(ctx, taskID, attempt)
		})
		return
	}
	r.clearRetrying(taskID)
}

// retryDispatchFailures 统计同一次重试尝试里下发失败的次数（并发安全）。
func (r *Reconciler) retryDispatchFailures(taskID string) int {
	// 简化：用 retrying 标记的时长近似失败次数不准确，
	// 这里用一个独立的计数器 map 跟踪。
	r.retryMu.Lock()
	defer r.retryMu.Unlock()
	r.dispatchFailures[taskID]++
	return r.dispatchFailures[taskID]
}

// clearRetrying 清除任务的重试去重标记和失败计数。
func (r *Reconciler) clearRetrying(taskID string) {
	r.retryMu.Lock()
	delete(r.retrying, taskID)
	delete(r.dispatchFailures, taskID)
	r.retryMu.Unlock()
}

// handleMissedSchedule 补偿漏调度：next_run_at 已过期但未执行（可能因 Worker 不足）。
// 注意：依赖未满足的任务（DAG 挂起）不做补偿，避免绕过依赖门控提前下发。
func (r *Reconciler) handleMissedSchedule(ctx context.Context, task *model.Task) {
	// 如果任务已经严重过期（> 5 分钟）且没有运行实例，说明漏调度了
	if time.Since(task.NextRunAt) > 5*time.Minute && !r.hasRunningInstance(ctx, task.ID) {
		if satisfied, _, hasDeps := r.depsSatisfied(ctx, task); hasDeps && !satisfied {
			// DAG 挂起中，保持阻塞，等待上游。
			return
		}
		metrics.RecordMissedSchedule()
		slog.Warn("missed schedule detected", "task_id", task.ID,
			"next_run_at", task.NextRunAt)
		// 立即补一次调度
		_ = r.dispatch(ctx, task, 0, true)
	}
}

// handleDeletion Finalizer 优雅删除。
func (r *Reconciler) handleDeletion(ctx context.Context, task *model.Task) error {
	slog.Info("handling task deletion", "task_id", task.ID)

	// 1. 找到该任务所有运行中的实例，通知 Worker 取消
	var runningLogs []model.TaskLog
	if err := r.db.WithContext(ctx).
		Where("task_id = ? AND status = ?", task.ID, model.TaskLogStatusRunning).
		Find(&runningLogs).Error; err != nil {
		return err
	}
	for i := range runningLogs {
		l := runningLogs[i]
		_ = r.executor.Cancel(ctx, l.InstanceID)
		r.db.WithContext(ctx).Model(&l).Update("status", model.TaskLogStatusCancelled)
	}

	// 2. 移除终结束 → 真正删除（软删）
	if err := r.db.WithContext(ctx).Unscoped().Delete(&model.Task{}, "id = ?", task.ID).Error; err != nil {
		return err
	}
	return nil
}

// setCondition 更新条件状态。
func (r *Reconciler) setCondition(ctx context.Context, taskID string,
	ctype model.ConditionType, status model.ConditionStatus, reason, message string) {

	var cond model.TaskCondition
	result := r.db.WithContext(ctx).
		Where("task_id = ? AND type = ?", taskID, string(ctype)).
		First(&cond)

	now := time.Now()
	if result.Error != nil {
		// 不存在 → 创建
		r.db.WithContext(ctx).Create(&model.TaskCondition{
			TaskID:       taskID,
			Type:         ctype,
			Status:       status,
			Reason:       reason,
			Message:      message,
			ObservedAt:   now,
			TransitionAt: now,
			CreatedAt:    now,
		})
		return
	}

	updates := map[string]any{
		"status":      string(status),
		"reason":      reason,
		"message":     message,
		"observed_at": now,
	}
	// 仅在状态变化时更新 transition_at
	if cond.Status != status {
		updates["transition_at"] = now
	}
	r.db.WithContext(ctx).Model(&cond).Updates(updates)
}

// EnsureValidCron 校验 cron 并返回下一次执行时间。
var _ = errors.New // placeholder

// ErrNilExecutor 未注入执行器。
var ErrNilExecutor = errors.New("nil executor")
