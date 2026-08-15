package executor

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yourname/dolphin/internal/pkg/metrics"
	"github.com/yourname/dolphin/internal/pkg/ratelog"
)

// resultLog 结果日志限频器。report 在 pool worker goroutine 里执行，
// 每个任务一条 slog 会让 50 个池槽位在写慢速 Windows 控制台时互相阻塞，
// 降低执行吞吐。完整状态在 task_logs，限频不影响排障。
var resultLog = ratelog.New(500 * time.Millisecond)

// TaskDispatch 下发的任务。
type TaskDispatch struct {
	TaskID     string
	InstanceID string
	Handler    string
	HandlerType string
	Params     string
	Timeout    int // 秒
}

// TaskResult 执行结果。
type TaskResult struct {
	InstanceID string
	TaskID     string
	Status     string // success/failed/timeout
	Result     string
	ErrorMsg   string
}

// ResultReporter 结果上报接口（由 Worker 客户端实现，通过 gRPC 回传 Scheduler）。
type ResultReporter interface {
	ReportResult(ctx context.Context, r *TaskResult) error
}

// Pool 固定并发度的协程池。
//
// 设计要点：
//   - capacity 个常驻 goroutine，避免每次任务都创建 goroutine。
//   - buffered channel（capacity*2）作为任务队列。
//   - 每个任务独立 context.WithTimeout，一个任务超时不影响其他任务。
//   - Submit 非阻塞：队列满则拒绝（Worker 负载已满，让调度器分给其他 Worker）。
type Pool struct {
	capacity int
	taskCh   chan *TaskDispatch
	reporter ResultReporter
	quit     chan struct{}
	wg       sync.WaitGroup

	currentLoad atomic.Int64
}

// NewPool 创建协程池。
func NewPool(capacity int, reporter ResultReporter) *Pool {
	p := &Pool{
		capacity: capacity,
		taskCh:   make(chan *TaskDispatch, capacity*2),
		reporter: reporter,
		quit:     make(chan struct{}),
	}
	for i := 0; i < capacity; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
	return p
}

// SetReporter 注入结果上报器（客户端连接建立后调用）。
func (p *Pool) SetReporter(r ResultReporter) {
	p.reporter = r
}

func (p *Pool) worker(id int) {
	defer p.wg.Done()
	for {
		select {
		case task := <-p.taskCh:
			p.execute(task)
		case <-p.quit:
			return
		}
	}
}

// Submit 提交任务。队列满返回 false（不阻塞）。
func (p *Pool) Submit(task *TaskDispatch) bool {
	select {
	case p.taskCh <- task:
		p.currentLoad.Add(1)
		metrics.SetWorkerPoolInflight(p.currentLoad.Load())
		return true
	default:
		return false
	}
}

// CurrentLoad 当前正在执行的任务数。
func (p *Pool) CurrentLoad() int {
	return int(p.currentLoad.Load())
}

// updatePoolMetrics 同步池利用率与在途数 gauge（currentLoad = 排队 + 执行）。
func (p *Pool) updatePoolMetrics() {
	load := p.currentLoad.Load()
	metrics.SetWorkerPoolInflight(load)
	metrics.WorkerPoolUtilization.Set(float64(load) / float64(p.capacity))
}

// execute 执行单个任务，带超时控制。
func (p *Pool) execute(task *TaskDispatch) {
	defer p.currentLoad.Add(-1)
	// 更新池利用率 + 在途数指标
	p.updatePoolMetrics()
	defer func() {
		p.updatePoolMetrics()
	}()

	metrics.RecordWorkerTaskStarted()
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(task.Timeout)*time.Second)
	defer cancel()

	done := make(chan *TaskResult, 1)
	go func() {
		done <- p.doExecute(ctx, task)
	}()

	select {
	case result := <-done:
		p.report(ctx, result)
		metrics.RecordWorkerTaskCompleted(task.HandlerType, time.Since(start), result.Status)
	case <-ctx.Done():
		p.report(ctx, &TaskResult{
			InstanceID: task.InstanceID,
			TaskID:     task.TaskID,
			Status:     "timeout",
			ErrorMsg:   "task execution timed out after " + (time.Duration(task.Timeout) * time.Second).String(),
		})
		metrics.RecordWorkerTaskCompleted(task.HandlerType, time.Since(start), "timeout")
	}
}

func (p *Pool) report(ctx context.Context, r *TaskResult) {
	if p.reporter == nil {
		return
	}
	resultLog.Info("task result (rate-limited)", "instance_id", r.InstanceID, "status", r.Status)
	if err := p.reporter.ReportResult(ctx, r); err != nil {
		slog.Warn("failed to report result", "instance_id", r.InstanceID, "err", err)
	}
}

// doExecute 按 handler_type 分发执行。
func (p *Pool) doExecute(ctx context.Context, task *TaskDispatch) *TaskResult {
	switch task.HandlerType {
	case "http":
		return p.executeHTTP(ctx, task)
	case "shell":
		return p.executeShell(ctx, task)
	default:
		return &TaskResult{
			InstanceID: task.InstanceID,
			TaskID:     task.TaskID,
			Status:     "failed",
			ErrorMsg:   fmt.Sprintf("unknown handler type: %s", task.HandlerType),
		}
	}
}

// executeHTTP 调用 HTTP 端点。
func (p *Pool) executeHTTP(ctx context.Context, task *TaskDispatch) *TaskResult {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, task.Handler, nil)
	if err != nil {
		return &TaskResult{
			InstanceID: task.InstanceID, TaskID: task.TaskID,
			Status: "failed", ErrorMsg: err.Error(),
		}
	}
	req.Header.Set("X-Dolphin-Instance", task.InstanceID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		status := "failed"
		if ctx.Err() != nil {
			status = "timeout"
		}
		return &TaskResult{
			InstanceID: task.InstanceID, TaskID: task.TaskID,
			Status: status, ErrorMsg: err.Error(),
		}
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 400 {
		return &TaskResult{
			InstanceID: task.InstanceID, TaskID: task.TaskID,
			Status: "failed",
			ErrorMsg: fmt.Sprintf("http %d: %s", resp.StatusCode, string(body)),
		}
	}
	return &TaskResult{
		InstanceID: task.InstanceID, TaskID: task.TaskID,
		Status: "success", Result: string(body),
	}
}

// executeShell 执行 shell 命令。
func (p *Pool) executeShell(ctx context.Context, task *TaskDispatch) *TaskResult {
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", task.Handler)
	out, err := cmd.CombinedOutput()
	if err != nil {
		status := "failed"
		if ctx.Err() != nil {
			status = "timeout"
		}
		return &TaskResult{
			InstanceID: task.InstanceID, TaskID: task.TaskID,
			Status: status, ErrorMsg: err.Error() + ": " + string(out),
		}
	}
	return &TaskResult{
		InstanceID: task.InstanceID, TaskID: task.TaskID,
		Status: "success", Result: string(out),
	}
}

// Shutdown 优雅关闭：停止接收新任务，等待执行中的任务完成。
func (p *Pool) Shutdown() {
	close(p.quit)
	p.wg.Wait()
}
