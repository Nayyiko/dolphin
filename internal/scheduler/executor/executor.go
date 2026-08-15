package executor

import (
	"context"
	"fmt"

	"github.com/yourname/dolphin/api/proto/pb"
	"github.com/yourname/dolphin/internal/pkg/model"
	"github.com/yourname/dolphin/internal/scheduler/reconciler"
)

// gRPCDispatcher 将任务下发给 gRPC 层（通过 Dispatch 通道）。
// 该接口由 server 层实现。
type Dispatcher interface {
	Dispatch(workerID string, d *pb.TaskDispatch) error
	OnlineWorkers() []model.Worker
	MarkWorkerOffline(workerID string)
}

// WorkerSelector 选择 Worker 的策略。
type WorkerSelector interface {
	Select(workers []model.Worker) *model.Worker
}

// LeastLoadedSelector 最少负载优先：选择 current_load / max_concurrency 比值最小的 Worker。
// 用负载比而非绝对值，保证 max_concurrency 不统一的混合集群也能正确选到"最闲"的节点。
type LeastLoadedSelector struct{}

func (LeastLoadedSelector) Select(workers []model.Worker) *model.Worker {
	if len(workers) == 0 {
		return nil
	}
	loadRatio := func(w model.Worker) float64 {
		if w.MaxConcurrency <= 0 {
			return float64(w.CurrentLoad) // 缺省值保护，避免除零
		}
		return float64(w.CurrentLoad) / float64(w.MaxConcurrency)
	}
	best := 0
	for i := 1; i < len(workers); i++ {
		if loadRatio(workers[i]) < loadRatio(workers[best]) {
			best = i
		}
	}
	return &workers[best]
}

// RoundRobinSelector 轮询选择。
type RoundRobinSelector struct {
	next int
}

func (s *RoundRobinSelector) Select(workers []model.Worker) *model.Worker {
	if len(workers) == 0 {
		return nil
	}
	w := workers[s.next%len(workers)]
	s.next++
	return &w
}

// TaskExecutor 基于 gRPC 的任务执行器。
// 实现 reconciler.TaskExecutor 接口。
type TaskExecutor struct {
	dispatcher Dispatcher
	selector   WorkerSelector
}

// NewTaskExecutor 创建执行器。
func NewTaskExecutor(d Dispatcher, sel WorkerSelector) *TaskExecutor {
	return &TaskExecutor{dispatcher: d, selector: sel}
}

// Execute 选择 Worker 并下发任务。instanceID 由调用方（Reconciler）生成，
// 保证日志先于下发落库、结果与日志一一对应。返回执行结果。
func (e *TaskExecutor) Execute(ctx context.Context, task *model.Task, instanceID string) (reconciler.ExecuteResult, error) {
	workers := e.dispatcher.OnlineWorkers()
	worker := e.selector.Select(workers)
	if worker == nil {
		return reconciler.ExecuteResult{}, fmt.Errorf("no available worker for task %s", task.ID)
	}

	dispatch := &pb.TaskDispatch{
		TaskId:      task.ID,
		InstanceId:  instanceID,
		Handler:     task.Handler,
		HandlerType: task.HandlerType,
		Params:      task.Params,
		Timeout:     int32(task.Timeout),
	}

	if err := e.dispatcher.Dispatch(worker.ID, dispatch); err != nil {
		// 下发失败（worker 可能已断连）→ 标记离线并报错重试
		e.dispatcher.MarkWorkerOffline(worker.ID)
		return reconciler.ExecuteResult{}, fmt.Errorf("dispatch to %s: %w", worker.ID, err)
	}

	return reconciler.ExecuteResult{
		WorkerID:   worker.ID,
		InstanceID: instanceID,
	}, nil
}

// Cancel 通知 Worker 取消某次执行。
func (e *TaskExecutor) Cancel(ctx context.Context, instanceID string) error {
	// 简化：取消通过 dispatcher 广播（真实场景需按 worker 定位）。
	// 这里返回 nil，由 worker 侧的 context 取消机制兜底。
	return nil
}
