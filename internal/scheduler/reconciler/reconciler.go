package reconciler

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/yourname/dolphin/internal/pkg/model"
	"github.com/yourname/dolphin/internal/scheduler/informer"
	"github.com/yourname/dolphin/internal/scheduler/manager"
	"github.com/yourname/dolphin/internal/scheduler/queue"
)

// DolphinFinalizer 终结器名称。任务删除时先执行清理，再移除终结器真正删除。
const DolphinFinalizer = "dolphin.io/task-cleanup"

// TaskExecutor 任务执行接口。
// 由 server 层实现（将任务下发给 Worker）。
type TaskExecutor interface {
	// Execute 将任务下发给 Worker。返回执行结果。
	Execute(ctx context.Context, task *model.Task) (ExecuteResult, error)
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

	mu     sync.Mutex
	running map[string]bool // taskID → 正在执行中（避免重复调度）
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
		queue:    q,
		lister:   lister,
		db:       mgr.DB(),
		manager:  mgr,
		executor: executor,
		cronNext: cronNext,
		workers:  numWorkers,
		running:  make(map[string]bool),
	}
}

// EnqueueTask 将任务推入协调队列（由 Informer 事件处理器调用）。
func (r *Reconciler) EnqueueTask(taskID string) {
	r.queue.Add(taskID)
}

// EnqueueAfter 延迟入队（预计算 cron 到期时间）。
func (r *Reconciler) EnqueueAfter(taskID string, delay time.Duration) {
	r.queue.AddAfter(taskID, delay)
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

func (r *Reconciler) worker(ctx context.Context) {
	for {
		key, shutdown := r.queue.Get()
		if shutdown {
			return
		}
		err := r.reconcile(ctx, key)
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
		if err := r.dispatch(ctx, &task); err != nil {
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

// dispatch 选择 Worker 并下发任务。
func (r *Reconciler) dispatch(ctx context.Context, task *model.Task) error {
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

	// 执行（下发 Worker）
	result, err := r.executor.Execute(ctx, task)
	if err != nil {
		return err
	}

	// 记录执行日志
	instanceID := result.InstanceID
	if instanceID == "" {
		instanceID = uuid.NewString()
	}
	now := time.Now()
	log := &model.TaskLog{
		ID:         uuid.NewString(),
		TaskID:     task.ID,
		InstanceID: instanceID,
		WorkerID:   result.WorkerID,
		Status:     model.TaskLogStatusRunning,
		StartTime:  now,
		RetryCount: 0,
		CreatedAt:  now,
	}
	if err := r.db.WithContext(ctx).Create(log).Error; err != nil {
		return err
	}

	// 预计算下一次触发时间
	if r.cronNext != nil {
		next, err := r.cronNext(task.CronExpr)
		if err == nil {
			if err := r.manager.UpdateNextRunAt(ctx, task.ID, next); err != nil {
				return err
			}
		}
	}

	// 更新 Conditions
	r.setCondition(ctx, task.ID, model.ConditionScheduled, model.StatusTrue,
		"Dispatched", "dispatched to worker "+result.WorkerID)
	r.setCondition(ctx, task.ID, model.ConditionRunning, model.StatusTrue,
		"Executing", "instance "+instanceID)

	return nil
}

// handleMissedSchedule 补偿漏调度：next_run_at 已过期但未执行（可能因 Worker 不足）。
func (r *Reconciler) handleMissedSchedule(ctx context.Context, task *model.Task) {
	// 如果任务已经严重过期（> 5 分钟）且没有运行实例，说明漏调度了
	if time.Since(task.NextRunAt) > 5*time.Minute && !r.hasRunningInstance(ctx, task.ID) {
		slog.Warn("missed schedule detected", "task_id", task.ID,
			"next_run_at", task.NextRunAt)
		// 立即补一次调度
		_ = r.dispatch(ctx, task)
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
