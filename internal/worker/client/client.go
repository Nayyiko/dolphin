package client

import (
	"context"
	"io"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/yourname/dolphin/api/proto/pb"
	"github.com/yourname/dolphin/internal/worker/executor"
)

// Client Worker gRPC 客户端。
// 负责与 Scheduler 建立双向流：
//   - 注册（Register）
//   - 心跳（Heartbeat，携带 current_load）
//   - 接收任务（TaskDispatch）→ 提交协程池
//   - 上报结果（TaskResult）
type Client struct {
	conn    *grpc.ClientConn
	stream  pb.Scheduler_ConnectClient
	pool    *executor.Pool
	workerID string
	addr    string
	maxConc int
}

// Config Worker 客户端配置。
type Config struct {
	SchedulerAddr string
	WorkerID      string
	Address       string // Worker 对外可访问地址（供调度器记录）
	MaxConcurrency int
}

// New 创建 Worker 客户端并建立连接。
func New(cfg Config, pool *executor.Pool) (*Client, error) {
	conn, err := grpc.Dial(cfg.SchedulerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16<<20)),
	)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:     conn,
		pool:     pool,
		workerID: cfg.WorkerID,
		addr:     cfg.Address,
		maxConc:  cfg.MaxConcurrency,
	}, nil
}

// Run 建立双向流并进入消息循环。阻塞直到 ctx 取消或连接断开。
func (c *Client) Run(ctx context.Context) error {
	client := pb.NewSchedulerClient(c.conn)

	// 建立双向流
	stream, err := client.Connect(ctx)
	if err != nil {
		return err
	}
	c.stream = stream

	// 注册
	if err := stream.Send(&pb.WorkerMessage{
		Msg: &pb.WorkerMessage_Register{
			Register: &pb.RegisterRequest{
				WorkerId:       c.workerID,
				Address:        c.addr,
				MaxConcurrency: int32(c.maxConc),
			},
		},
	}); err != nil {
		return err
	}

	// 启动心跳
	go c.heartbeatLoop(ctx)

	// 接收消息循环
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch msg := in.Msg.(type) {
		case *pb.SchedulerMessage_RegisterAck:
			slog.Info("registered with scheduler", "accepted", msg.RegisterAck.Accepted)
		case *pb.SchedulerMessage_Dispatch:
			c.handleDispatch(msg.Dispatch)
		case *pb.SchedulerMessage_Cancel:
			slog.Info("cancel requested", "instance_id", msg.Cancel.InstanceId)
		}
	}
}

// heartbeatLoop 周期心跳。
func (c *Client) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			load := c.pool.CurrentLoad()
			if err := c.stream.Send(&pb.WorkerMessage{
				Msg: &pb.WorkerMessage_Heartbeat{
					Heartbeat: &pb.Heartbeat{
						WorkerId:    c.workerID,
						CurrentLoad: int32(load),
					},
				},
			}); err != nil {
				slog.Warn("heartbeat send failed", "err", err)
				return
			}
		}
	}
}

// handleDispatch 处理下发的任务。
func (c *Client) handleDispatch(d *pb.TaskDispatch) {
	accepted := c.pool.Submit(&executor.TaskDispatch{
		TaskID:      d.TaskId,
		InstanceID:  d.InstanceId,
		Handler:     d.Handler,
		HandlerType: d.HandlerType,
		Params:      d.Params,
		Timeout:     int(d.Timeout),
	})
	if !accepted {
		slog.Warn("pool full, rejecting task", "instance_id", d.InstanceId)
		// 队列满：立即上报失败，让调度器分给其他 Worker 或重试
		_ = c.stream.Send(&pb.WorkerMessage{
			Msg: &pb.WorkerMessage_TaskResult{
				TaskResult: &pb.TaskResult{
					InstanceId: d.InstanceId,
					TaskId:     d.TaskId,
					WorkerId:   c.workerID,
					Status:     "failed",
					ErrorMsg:   "worker pool full",
				},
			},
		})
	}
}

// ReportResult 上报任务执行结果（实现 executor.ResultReporter）。
func (c *Client) ReportResult(ctx context.Context, r *executor.TaskResult) error {
	return c.stream.Send(&pb.WorkerMessage{
		Msg: &pb.WorkerMessage_TaskResult{
			TaskResult: &pb.TaskResult{
				InstanceId: r.InstanceID,
				TaskId:     r.TaskID,
				WorkerId:   c.workerID,
				Status:     r.Status,
				Result:     r.Result,
				ErrorMsg:   r.ErrorMsg,
			},
		},
	})
}

// Close 关闭连接。
func (c *Client) Close() error {
	return c.conn.Close()
}
