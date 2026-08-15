package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/yourname/dolphin/api/proto/pb"
	"github.com/yourname/dolphin/internal/worker/executor"
)

// TestMaybeResolveThreshold 验证 Resolver 只在 backoff 增长到阈值后才被调用，
// 避免正常抖动时对 etcd 造成无谓压力。
func TestMaybeResolveThreshold(t *testing.T) {
	var calls int32
	c := &Client{Resolver: func() { atomic.AddInt32(&calls, 1) }}

	c.maybeResolve(1 * time.Second)
	c.maybeResolve(2 * time.Second)
	c.maybeResolve(4 * time.Second)
	if n := atomic.LoadInt32(&calls); n != 0 {
		t.Fatalf("expected no resolve below threshold, got %d", n)
	}

	c.maybeResolve(8 * time.Second)
	c.maybeResolve(16 * time.Second)
	c.maybeResolve(30 * time.Second)
	if n := atomic.LoadInt32(&calls); n != 3 {
		t.Fatalf("expected 3 resolves at/above threshold, got %d", n)
	}
}

// TestMaybeResolveNilResolver nil Resolver 不应 panic。
func TestMaybeResolveNilResolver(t *testing.T) {
	c := &Client{}
	c.maybeResolve(30 * time.Second)
}

// ---------- 结果上报缓冲 + 重试 ----------

// fakeStream 内存版双向流：记录 Send 的消息；failFirst 使前 N 次 Send 失败，
// 模拟连接断开/Leader 切换后的重连。
type fakeStream struct {
	grpc.ClientStream
	mu        sync.Mutex
	sends     []*pb.WorkerMessage
	failFirst int
	failCnt   int
}

func (f *fakeStream) Send(m *pb.WorkerMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failCnt < f.failFirst {
		f.failCnt++
		return errors.New("stream broken")
	}
	f.sends = append(f.sends, m)
	return nil
}

func (f *fakeStream) Recv() (*pb.SchedulerMessage, error) {
	return nil, io.EOF
}

func (f *fakeStream) msg(i int) *pb.TaskResult { return f.sends[i].GetTaskResult() }

func newResult(id string) *executor.TaskResult {
	return &executor.TaskResult{InstanceID: id, TaskID: "t1", Status: "success", Result: "ok"}
}

// TestReportResultQueueBackpressure 结果缓冲队列满时非阻塞返回错误，
// 不会卡死执行池（背压而不是堆积）。
func TestReportResultQueueBackpressure(t *testing.T) {
	c := &Client{resultCh: make(chan *executor.TaskResult, 2)}
	if err := c.ReportResult(context.Background(), newResult("a")); err != nil {
		t.Fatalf("first enqueue should succeed: %v", err)
	}
	if err := c.ReportResult(context.Background(), newResult("b")); err != nil {
		t.Fatalf("second enqueue should succeed: %v", err)
	}
	if err := c.ReportResult(context.Background(), newResult("c")); err == nil {
		t.Fatal("third enqueue should fail with queue full")
	}
}

// TestSendResultOnceNilStream 连接尚未就绪（stream=nil）→ 返回 false，
// 由 drainResults 负责退避重试，结果不丢。
func TestSendResultOnceNilStream(t *testing.T) {
	c := &Client{} // stream == nil
	if c.sendResultOnce(newResult("a")) {
		t.Fatal("expected false when stream is nil")
	}
}

// TestSendResultWithRetryRecovers 发送失败后按退避重试，连接恢复后送达。
func TestSendResultWithRetryRecovers(t *testing.T) {
	fs := &fakeStream{failFirst: 1}
	c := &Client{stream: fs, workerID: "w1"}
	c.sendResultWithRetry(context.Background(), newResult("inst-1"))

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.sends) != 1 {
		t.Fatalf("expected 1 send after recovery, got %d", len(fs.sends))
	}
	tr := fs.msg(0)
	if tr.InstanceId != "inst-1" || tr.Status != "success" {
		t.Fatalf("unexpected result message: %+v", tr)
	}
}

// TestDrainResultsFlushesRemaining 优雅关闭（ctx 取消）时，把已在队列中的
// 剩余结果在有限窗口内全部发出，避免结果丢失。
func TestDrainResultsFlushesRemaining(t *testing.T) {
	fs := &fakeStream{}
	c := &Client{stream: fs, workerID: "w1", resultCh: make(chan *executor.TaskResult, 256)}
	for i := 0; i < 3; i++ {
		c.resultCh <- newResult(fmt.Sprintf("inst-%d", i))
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.drainResults(ctx)

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.sends) != 3 {
		t.Fatalf("expected 3 results flushed on shutdown, got %d", len(fs.sends))
	}
}

// TestDrainResultsBlocksUntilResult 主循环阻塞等结果：入队后应被发送
// （验证异步上报路径：执行完的结果不阻塞执行池）。
func TestDrainResultsBlocksUntilResult(t *testing.T) {
	fs := &fakeStream{}
	c := &Client{stream: fs, workerID: "w1", resultCh: make(chan *executor.TaskResult, 8)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		c.drainResults(ctx)
		close(done)
	}()

	c.ReportResult(context.Background(), newResult("inst-x"))

	deadline := time.After(2 * time.Second)
	for {
		fs.mu.Lock()
		n := len(fs.sends)
		fs.mu.Unlock()
		if n >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for result to be sent")
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("drainResults did not stop after cancel")
	}
}
