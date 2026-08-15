package client

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/yourname/dolphin/api/proto/pb"
	"github.com/yourname/dolphin/internal/pkg/metrics"
	"github.com/yourname/dolphin/internal/pkg/ratelog"
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

	// Resolver, when set, is invoked after repeated connection failures to
	// re-resolve the current leader address (e.g. re-read the etcd leader-addr
	// key). This closes the failover loop: if the leader-addr key goes stale
	// (old leader's lease not yet expired, watch event missed), the worker can
	// actively re-discover instead of retrying a dead address forever.
	Resolver func()

	// sendMu 串行化同一 stream 的 Send，避免心跳与结果上报并发写同一流。
	sendMu sync.Mutex
	// hbCancel 用于停止当前连接的心跳循环（重连时先停旧心跳再建新连接）。
	hbCancel context.CancelFunc
	// reconnectCh 通知 Run 循环"目标地址变了，立即重连"。
	reconnectCh chan struct{}

	// resultCh 执行结果缓冲队列。结果先入队由后台 drainResults 发送，
	// 发送失败（断线/Leader 切换）时退避重试，保证结果不因瞬时连接抖动丢失。
	// 容量足够容纳在途上限（capacity + capacity*2）。
	resultCh chan *executor.TaskResult

	// rejectLog 池满拒绝日志限频器。handleDispatch 在 worker 的 recv 循环里，
	// 对每条拒绝同步写控制台会阻塞 recvLoop → 停止 Recv 下发 → 拖垮调度。
	rejectLog *ratelog.Logger
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
		resultCh:    make(chan *executor.TaskResult, 256),
		rejectLog:   ratelog.New(500 * time.Millisecond),
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

	// 启动结果上报排空循环：结果发送失败时退避重试，跨连接保留直到送达。
	// 与 Run 共用 ctx：优雅关闭时排空剩余结果（见 drainResults）。
	go c.drainResults(ctx)

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
			c.maybeResolve(backoff)
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
			c.maybeResolve(backoff)
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
			c.maybeResolve(backoff)
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
		// 队列满：立即上报失败，让调度器分给其他 Worker 或重试。
		metrics.RecordPoolRejected()
		c.rejectLog.Warn("pool full, rejecting task (rate-limited)", "instance_id", d.InstanceId)
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
// 结果先入队，由后台 drainResults 发送；发送失败会退避重试，
// 不会因为瞬时断线/Leader 切换而丢失。
func (c *Client) ReportResult(_ context.Context, r *executor.TaskResult) error {
	select {
	case c.resultCh <- r:
		return nil
	default:
		return errors.New("result queue full")
	}
}

// drainResults 从结果队列取结果并发送。单 goroutine 保证同一任务多实例按序上报。
// 发送失败（stream 未就绪/连接断开）时指数退避重试，直到送达或 ctx 取消。
func (c *Client) drainResults(ctx context.Context) {
	for {
		select {
		case r := <-c.resultCh:
			c.sendResultWithRetry(ctx, r)
		case <-ctx.Done():
			// 优雅关闭：在有限窗口内尽力排空剩余结果。
			// 调用方先 pool.Shutdown()（结果全部入队）再 cancel()，因此
			// 这里能拿到全部在途结果；发不完的由调度器 stale-running 兜底。
			deadline := time.After(5 * time.Second)
			for {
				select {
				case r := <-c.resultCh:
					c.sendResultOnce(r)
				case <-deadline:
					return
				default:
					return
				}
			}
		}
	}
}

// sendResultWithRetry 发送一个结果，失败则退避重试。
// 注意：先尝试发送再检查 ctx——优雅关闭时已在手的单个结果优先送出去，
// 而不是因为 ctx 已取消就丢弃（剩余排队的由 drainResults 的 flush 窗口兜底）。
func (c *Client) sendResultWithRetry(ctx context.Context, r *executor.TaskResult) {
	backoff := 500 * time.Millisecond
	const maxBackoff = 30 * time.Second
	for {
		if c.sendResultOnce(r) {
			return
		}
		select {
		case <-ctx.Done():
			return // 连接也发不出去，放弃（调度器 stale-running 兜底）
		default:
		}
		if !sleep(ctx, backoff) {
			return
		}
		if backoff < maxBackoff {
			backoff *= 2
		}
	}
}

// sendResultOnce 尝试发送一个结果。返回是否成功。
func (c *Client) sendResultOnce(r *executor.TaskResult) bool {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if c.stream == nil {
		return false // 连接尚未就绪
	}
	err := c.stream.Send(&pb.WorkerMessage{
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
	if err != nil {
		slog.Warn("send result failed, will retry", "instance_id", r.InstanceID, "err", err)
		return false
	}
	return true
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

// maybeResolve 在连续连接失败（backoff 已增长到 ≥8s）时，主动触发地址重解析。
// 用于 Leader 已切换但 etcd watch 事件延迟/遗漏的场景，避免对死地址无限重试。
// 限频：backoff 越大调用越稀疏，正常抖动时（1s/2s/4s）不触发。
func (c *Client) maybeResolve(backoff time.Duration) {
	if c.Resolver == nil || backoff < 8*time.Second {
		return
	}
	c.Resolver()
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
