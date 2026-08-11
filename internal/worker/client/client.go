package client

import (
	"context"
	"io"
	"log/slog"
	"sync"
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
//
// 具备自动重连能力：Scheduler 未就绪或连接中断时，指数退避重试，
// 保证组件启动顺序不确定时也能正常工作。
//
// 支持 Leader 自动切换：当 etcd 发现新的 Scheduler Leader 时，
// 调用 UpdateAddr 更新目标地址，Run 循环会立即断开旧连接并连到新 Leader。
type Client struct {
	mu       sync.RWMutex
	addr     string // 当前 Scheduler Leader 的 gRPC 地址（可切换）
	ownAddr  string // Worker 自身对外地址（注册时上报给调度器）
	conn     *grpc.ClientConn
	stream   pb.Scheduler_ConnectClient
	pool     *executor.Pool
	workerID string
	maxConc  int

	// sendMu 串行化同一 stream 的 Send，避免心跳与结果上报并发写同一流。
	sendMu sync.Mutex
	// hbCancel 用于停止当前连接的心跳循环（重连时先停旧心跳再建新连接）。
	hbCancel context.CancelFunc
	// reconnectCh 通知 Run 循环"目标地址变了，立即重连"。
	reconnectCh chan struct{}
}

// Config Worker 客户端配置。
type Config struct {
	SchedulerAddr  string
	WorkerID       string
	Address        string // Worker 对外可访问地址（供调度器记录）
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
		conn:        conn,
		pool:        pool,
		workerID:    cfg.WorkerID,
		addr:        cfg.SchedulerAddr,
		ownAddr:     cfg.Address,
		maxConc:     cfg.MaxConcurrency,
		reconnectCh: make(chan struct{}, 1),
	}, nil
}

// UpdateAddr 切换目标 Scheduler 地址（Leader 变化时由 discovery 调用）。
// 会触发重连：关闭旧连接，中断当前流，重新建立到新地址的连接。
func (c *Client) UpdateAddr(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if addr == "" || addr == c.addr {
		return
	}
	slog.Info("switching scheduler leader addr", "old", c.addr, "new", addr)
	c.addr = addr
	// 关闭旧连接，确保 ensureConn 会重新 Dial 到新地址。
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	select {
	case c.reconnectCh <- struct{}{}:
	default:
	}
}

// Run 建立双向流并进入消息循环，带自动重连 + Leader 地址切换。
// 仅在 ctx 取消（优雅关闭）时返回；连接错误会指数退避重试。
func (c *Client) Run(ctx context.Context) error {
	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// 读取当前目标地址，并确保 conn 指向它。
		// ensureConn 内部读取最新的 c.addr，避免拿到切换前的旧地址。
		c.mu.RLock()
		addr := c.addr
		c.mu.RUnlock()

		if err := c.ensureConn(); err != nil {
			slog.Warn("dial scheduler failed, retrying",
				"err", err, "backoff", backoff)
			if !sleep(ctx, backoff) {
				return ctx.Err()
			}
			if backoff < maxBackoff {
				backoff *= 2
			}
			continue
		}

		client := pb.NewSchedulerClient(c.conn)

		// 建立双向流
		stream, err := client.Connect(ctx)
		if err != nil {
			slog.Warn("connect to scheduler failed, retrying",
				"err", err, "backoff", backoff)
			if !sleep(ctx, backoff) {
				return ctx.Err()
			}
			if backoff < maxBackoff {
				backoff *= 2
			}
			continue
		}

		// 注册
		if err := stream.Send(&pb.WorkerMessage{
			Msg: &pb.WorkerMessage_Register{
				Register: &pb.RegisterRequest{
					WorkerId:       c.workerID,
					Address:        c.ownAddr,
					MaxConcurrency: int32(c.maxConc),
				},
			},
		}); err != nil {
			slog.Warn("register failed, retrying", "err", err)
			_ = stream.CloseSend()
			if !sleep(ctx, backoff) {
				return ctx.Err()
			}
			if backoff < maxBackoff {
				backoff *= 2
			}
			continue
		}

		// 连接成功：更新当前流，重启心跳（先停旧的）。
		// 锁约定：hbCancel / addr / conn 一律在 c.mu 下访问；
		// stream 只在 sendMu 下访问（因为 Send 操作本就持 sendMu）。
		c.mu.Lock()
		if c.hbCancel != nil {
			c.hbCancel()
		}
		hbCtx, hbCancel := context.WithCancel(ctx)
		c.hbCancel = hbCancel
		c.mu.Unlock()

		c.sendMu.Lock()
		c.stream = stream
		c.sendMu.Unlock()

		go c.heartbeatLoop(hbCtx, stream)

		slog.Info("connected to scheduler", "addr", addr)

		// 接收消息循环（阻塞直到连接断开或地址切换）
		recvErr := c.recvLoop(ctx, stream)
		hbCancel() // 停止本连接的心跳

		if recvErr == io.EOF || recvErr == nil {
			slog.Info("connection to scheduler closed, reconnecting")
		} else if ctx.Err() == nil {
			slog.Warn("connection to scheduler lost, reconnecting", "err", recvErr)
		}

		// 地址切换了？立即重连（不等退避）
		c.mu.RLock()
		addrChanged := c.addr != addr
		c.mu.RUnlock()

		backoff = time.Second
		if addrChanged {
			slog.Info("leader addr changed, reconnecting immediately")
			continue
		}
		if !sleep(ctx, 2*time.Second) {
			return ctx.Err()
		}
	}
}

// ensureConn 保证 conn 指向当前 c.addr。地址变化时关闭旧连接、新建连接。
func (c *Client) ensureConn() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 已有连接且指向当前地址：复用。
	if c.conn != nil {
		return nil
	}

	conn, err := grpc.Dial(c.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16<<20)),
	)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

// recvLoop 接收消息循环。
func (c *Client) recvLoop(ctx context.Context, stream pb.Scheduler_ConnectClient) error {
	// 地址切换通知：主动断开当前流，让外层重连。
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-c.reconnectCh:
			_ = stream.CloseSend()
		case <-done:
		case <-ctx.Done():
		}
	}()

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return io.EOF
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

// heartbeatLoop 周期心跳。绑定到指定 stream，连接断开即退出。
func (c *Client) heartbeatLoop(ctx context.Context, stream pb.Scheduler_ConnectClient) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			load := c.pool.CurrentLoad()
			c.sendMu.Lock()
			err := stream.Send(&pb.WorkerMessage{
				Msg: &pb.WorkerMessage_Heartbeat{
					Heartbeat: &pb.Heartbeat{
						WorkerId:    c.workerID,
						CurrentLoad: int32(load),
					},
				},
			})
			c.sendMu.Unlock()
			if err != nil {
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
		c.sendMu.Lock()
		if c.stream != nil {
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
		c.sendMu.Unlock()
	}
}

// ReportResult 上报任务执行结果（实现 executor.ResultReporter）。
func (c *Client) ReportResult(ctx context.Context, r *executor.TaskResult) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if c.stream == nil {
		return io.ErrClosedPipe
	}
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.hbCancel != nil {
		c.hbCancel()
		c.hbCancel = nil
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// sleep 返回 true 表示正常睡满，false 表示 ctx 已取消。
func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}
