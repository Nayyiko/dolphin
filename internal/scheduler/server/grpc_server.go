package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/yourname/dolphin/api/proto/pb"
	"github.com/yourname/dolphin/internal/pkg/model"
	"github.com/yourname/dolphin/internal/scheduler/manager"
)

// SchedulerService 实现 pb.SchedulerServer。
type SchedulerService struct {
	pb.UnimplementedSchedulerServer

	manager *manager.Manager
	cron    func(expr string) (time.Time, error) // cron 求值函数（注入避免循环依赖）

	// workers: workerID → 出站流
	mu      sync.RWMutex
	workers map[string]*workerConn
	// dispatchCh: 待下发的任务（由 Reconciler 写入）
	dispatchCh chan *pb.TaskDispatch
}

type workerConn struct {
	id       string
	stream   pb.Scheduler_ConnectServer
	sendMu   sync.Mutex // 串行化写入同一流
	lastSeen time.Time
}

// NewSchedulerService 创建 gRPC 服务。
func NewSchedulerService(mgr *manager.Manager) *SchedulerService {
	return &SchedulerService{
		manager:    mgr,
		workers:    make(map[string]*workerConn),
		dispatchCh: make(chan *pb.TaskDispatch, 1024),
	}
}

// SetCronNext 注入 cron 求值函数（返回下一次触发时间）。
func (s *SchedulerService) SetCronNext(fn func(expr string) (time.Time, error)) {
	s.cron = fn
}

// Dispatch 向指定 worker 下发任务（供 Reconciler 调用）。
func (s *SchedulerService) Dispatch(workerID string, d *pb.TaskDispatch) error {
	s.mu.RLock()
	wc, ok := s.workers[workerID]
	s.mu.RUnlock()
	if !ok {
		return fmt.Errorf("worker %s not connected", workerID)
	}
	wc.sendMu.Lock()
	defer wc.sendMu.Unlock()
	if err := wc.stream.Send(&pb.SchedulerMessage{
		Msg: &pb.SchedulerMessage_Dispatch{Dispatch: d},
	}); err != nil {
		return err
	}
	return nil
}

// WorkersOnline 返回在线 worker 数量。
func (s *SchedulerService) WorkersOnline() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.workers)
}

// Connect 处理 Worker 双向流。
func (s *SchedulerService) Connect(stream pb.Scheduler_ConnectServer) error {
	var wc *workerConn
	defer func() {
		// 连接断开：清理内存中的 worker 注册，避免把任务发给已断开的流
		if wc != nil {
			s.mu.Lock()
			if cur, ok := s.workers[wc.id]; ok && cur == wc {
				delete(s.workers, wc.id)
			}
			s.mu.Unlock()
			slog.Warn("worker disconnected, removed from registry", "worker_id", wc.id)
		}
	}()

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch msg := in.Msg.(type) {
		case *pb.WorkerMessage_Register:
			// 注册：建立 workerConn
			wc = &workerConn{
				id:       msg.Register.WorkerId,
				stream:   stream,
				lastSeen: time.Now(),
			}
			s.mu.Lock()
			s.workers[msg.Register.WorkerId] = wc
			s.mu.Unlock()
			slog.Info("worker registered", "worker_id", msg.Register.WorkerId,
				"addr", msg.Register.Address, "max_concurrency", msg.Register.MaxConcurrency)

			// 持久化 worker 到 MySQL
			s.upsertWorker(context.Background(), msg.Register)
			_ = stream.Send(&pb.SchedulerMessage{
				Msg: &pb.SchedulerMessage_RegisterAck{
					RegisterAck: &pb.RegisterResponse{Accepted: true, Message: "welcome"},
				},
			})

		case *pb.WorkerMessage_Heartbeat:
			if wc != nil {
				wc.lastSeen = time.Now()
			}
			s.updateWorkerHeartbeat(context.Background(), msg.Heartbeat)
			_ = stream.Send(&pb.SchedulerMessage{
				Msg: &pb.SchedulerMessage_HeartbeatAck{HeartbeatAck: &pb.HeartbeatAck{Ok: true}},
			})

		case *pb.WorkerMessage_TaskResult:
			s.handleTaskResult(context.Background(), msg.TaskResult)
		}
	}
}

// upsertWorker 写入/更新 worker 记录。
func (s *SchedulerService) upsertWorker(ctx context.Context, reg *pb.RegisterRequest) {
	db := s.manager.DB()
	var w model.Worker
	result := db.WithContext(ctx).First(&w, "id = ?", reg.WorkerId)
	if result.Error != nil {
		// 不存在 → 创建
		w = model.Worker{
			ID:             reg.WorkerId,
			Address:        reg.Address,
			Status:         model.WorkerStatusOnline,
			MaxConcurrency: int(reg.MaxConcurrency),
			CurrentLoad:    0,
			LastHeartbeat:  time.Now(),
			RegisteredAt:   time.Now(),
			UpdatedAt:      time.Now(),
		}
		_ = db.WithContext(ctx).Create(&w)
		return
	}
	db.WithContext(ctx).Model(&w).Updates(map[string]any{
		"address":         reg.Address,
		"status":          model.WorkerStatusOnline,
		"max_concurrency": reg.MaxConcurrency,
		"last_heartbeat":  time.Now(),
		"updated_at":      time.Now(),
	})
}

// updateWorkerHeartbeat 更新 worker 心跳和负载。
func (s *SchedulerService) updateWorkerHeartbeat(ctx context.Context, hb *pb.Heartbeat) {
	db := s.manager.DB()
	db.WithContext(ctx).Model(&model.Worker{}).
		Where("id = ?", hb.WorkerId).
		Updates(map[string]any{
			"status":         model.WorkerStatusOnline,
			"current_load":   hb.CurrentLoad,
			"last_heartbeat": time.Now(),
			"updated_at":     time.Now(),
		})
}

// handleTaskResult 处理任务执行结果，更新 task_log。
func (s *SchedulerService) handleTaskResult(ctx context.Context, r *pb.TaskResult) {
	db := s.manager.DB()
	slog.Info("task result received", "instance_id", r.InstanceId, "status", r.Status)

	updates := map[string]any{
		"status": r.Status,
	}
	now := time.Now()
	updates["end_time"] = &now
	if r.ErrorMsg != "" {
		updates["error_msg"] = r.ErrorMsg
	} else {
		updates["result"] = r.Result
	}

	db.WithContext(ctx).Model(&model.TaskLog{}).
		Where("instance_id = ?", r.InstanceId).
		Updates(updates)
}

// OnlineWorkers 获取在线 worker 列表（地址+负载），供 dispatcher 选择。
func (s *SchedulerService) OnlineWorkers() []model.Worker {
	db := s.manager.DB()
	var workers []model.Worker
	db.Where("status = ?", model.WorkerStatusOnline).Find(&workers)
	return workers
}

// MarkWorkerOffline 将 worker 标记为离线。
func (s *SchedulerService) MarkWorkerOffline(workerID string) {
	db := s.manager.DB()
	db.Model(&model.Worker{}).Where("id = ?", workerID).
		Update("status", model.WorkerStatusOffline)

	s.mu.Lock()
	delete(s.workers, workerID)
	s.mu.Unlock()
}

// ==================== Task 管理 API ====================

// CreateTask 创建任务。
func (s *SchedulerService) CreateTask(ctx context.Context, req *pb.CreateTaskRequest) (*pb.Task, error) {
	// 校验 cron 表达式
	if s.cron != nil {
		if _, err := s.cron(req.CronExpr); err != nil {
			return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid cron: %v", err))
		}
	}

	task := &model.Task{
		Name:        req.Name,
		CronExpr:    req.CronExpr,
		Handler:     req.Handler,
		HandlerType: req.HandlerType,
		Params:      req.Params,
		Timeout:     int(req.Timeout),
		MaxRetries:  int(req.MaxRetries),
	}
	// 首次触发时间 = cron 的下一次执行时间
	if s.cron != nil {
		next, err := s.cron(req.CronExpr)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		task.NextRunAt = next
	} else {
		task.NextRunAt = time.Now().Add(time.Minute)
	}

	created, err := s.manager.Create(ctx, task)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return toProtoTask(created), nil
}

// GetTask 获取任务。
func (s *SchedulerService) GetTask(ctx context.Context, req *pb.GetTaskRequest) (*pb.Task, error) {
	t, err := s.manager.Get(ctx, req.Id)
	if err != nil {
		if err == manager.ErrNotFound {
			return nil, status.Error(codes.NotFound, "task not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return toProtoTask(t), nil
}

// ListTasks 列出任务。
func (s *SchedulerService) ListTasks(ctx context.Context, req *pb.ListTasksRequest) (*pb.ListTasksResponse, error) {
	tasks, total, err := s.manager.List(ctx, req.Status, int(req.Offset), int(req.Limit))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &pb.ListTasksResponse{Total: total}
	for i := range tasks {
		resp.Tasks = append(resp.Tasks, toProtoTask(&tasks[i]))
	}
	return resp, nil
}

// UpdateTask 更新任务。
func (s *SchedulerService) UpdateTask(ctx context.Context, req *pb.UpdateTaskRequest) (*pb.Task, error) {
	task := &model.Task{
		ID:          req.Id,
		Name:        req.Name,
		CronExpr:    req.CronExpr,
		Handler:     req.Handler,
		HandlerType: req.HandlerType,
		Params:      req.Params,
		Timeout:     int(req.Timeout),
		MaxRetries:  int(req.MaxRetries),
	}
	if s.cron != nil {
		next, err := s.cron(req.CronExpr)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		task.NextRunAt = next
	}
	updated, err := s.manager.Update(ctx, task)
	if err != nil {
		if err == manager.ErrNotFound {
			return nil, status.Error(codes.NotFound, "task not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return toProtoTask(updated), nil
}

// DeleteTask 软删除任务。
func (s *SchedulerService) DeleteTask(ctx context.Context, req *pb.DeleteTaskRequest) (*pb.Empty, error) {
	if err := s.manager.Delete(ctx, req.Id); err != nil {
		if err == manager.ErrNotFound {
			return nil, status.Error(codes.NotFound, "task not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.Empty{}, nil
}

// PauseTask 暂停任务。
func (s *SchedulerService) PauseTask(ctx context.Context, req *pb.PauseTaskRequest) (*pb.Task, error) {
	t, err := s.manager.SetStatus(ctx, req.Id, model.TaskStatusPaused)
	if err != nil {
		if err == manager.ErrNotFound {
			return nil, status.Error(codes.NotFound, "task not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return toProtoTask(t), nil
}

// ResumeTask 恢复任务。
func (s *SchedulerService) ResumeTask(ctx context.Context, req *pb.ResumeTaskRequest) (*pb.Task, error) {
	t, err := s.manager.SetStatus(ctx, req.Id, model.TaskStatusActive)
	if err != nil {
		if err == manager.ErrNotFound {
			return nil, status.Error(codes.NotFound, "task not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return toProtoTask(t), nil
}

// TriggerTask 手动触发任务：设置 next_run_at=now，让 Reconciler 下一轮调度。
func (s *SchedulerService) TriggerTask(ctx context.Context, req *pb.TriggerTaskRequest) (*pb.Empty, error) {
	if err := s.manager.UpdateNextRunAt(ctx, req.Id, time.Now()); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.Empty{}, nil
}

// GetTaskLogs 查询执行历史。
func (s *SchedulerService) GetTaskLogs(ctx context.Context, req *pb.GetTaskLogsRequest) (*pb.GetTaskLogsResponse, error) {
	db := s.manager.DB()
	var logs []model.TaskLog
	q := db.WithContext(ctx).Where("task_id = ?", req.TaskId)
	if req.Limit <= 0 {
		req.Limit = 20
	}
	if err := q.Order("start_time DESC").Limit(int(req.Limit)).Find(&logs).Error; err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &pb.GetTaskLogsResponse{}
	for i := range logs {
		l := logs[i]
		entry := &pb.TaskLogEntry{
			Id:         l.ID,
			TaskId:     l.TaskID,
			InstanceId: l.InstanceID,
			WorkerId:   l.WorkerID,
			Status:     l.Status,
			StartTime:  l.StartTime.Unix(),
			Result:     l.Result,
			ErrorMsg:   l.ErrorMsg,
			RetryCount: int32(l.RetryCount),
		}
		if l.EndTime != nil {
			entry.EndTime = l.EndTime.Unix()
		}
		resp.Logs = append(resp.Logs, entry)
	}
	return resp, nil
}

// toProtoTask 转换 model.Task → pb.Task。
func toProtoTask(t *model.Task) *pb.Task {
	return &pb.Task{
		Id:          t.ID,
		Name:        t.Name,
		CronExpr:    t.CronExpr,
		NextRunAt:   t.NextRunAt.Unix(),
		Handler:     t.Handler,
		HandlerType: t.HandlerType,
		Params:      t.Params,
		Timeout:     int32(t.Timeout),
		MaxRetries:  int32(t.MaxRetries),
		Status:      t.Status,
		CreatedAt:   t.CreatedAt.Unix(),
		UpdatedAt:   t.UpdatedAt.Unix(),
	}
}

// RegisterServer 注册 gRPC 服务。
func (s *SchedulerService) RegisterServer(grpcServer *grpc.Server) {
	pb.RegisterSchedulerServer(grpcServer, s)
}
