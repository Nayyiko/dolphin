package queue

import (
	"context"
	"testing"
	"time"
)

func TestWorkQueue_Basic(t *testing.T) {
	q := NewWorkQueue(time.Millisecond, time.Second)

	q.Add("a")
	q.Add("b")

	got := make(map[string]bool)
	for i := 0; i < 2; i++ {
		item, shutdown := q.Get()
		if shutdown {
			t.Fatalf("unexpected shutdown")
		}
		got[item] = true
		q.Done(item)
	}
	if !got["a"] || !got["b"] {
		t.Fatalf("got %v", got)
	}
}

func TestWorkQueue_Dedup(t *testing.T) {
	q := NewWorkQueue(time.Millisecond, time.Second)

	q.Add("a")
	q.Add("a")
	q.Add("a")

	item, shutdown := q.Get()
	if shutdown || item != "a" {
		t.Fatalf("expected a, got %q", item)
	}
	q.Done(item)

	// 全部完成后队列应为空
	if q.Len() != 0 {
		t.Fatalf("queue should be empty after dedup + done, len=%d", q.Len())
	}
}

func TestWorkQueue_DirtyRequeue(t *testing.T) {
	q := NewWorkQueue(time.Millisecond, time.Second)

	q.Add("a")
	item, _ := q.Get() // 取出但未 Done

	// 处理期间再次 Add → dirty=true
	q.Add("a")

	// Done 后应自动重新入队
	q.Done(item)

	next, shutdown := q.Get()
	if shutdown {
		t.Fatalf("unexpected shutdown")
	}
	if next != "a" {
		t.Fatalf("expected requeue of a, got %q", next)
	}
	q.Done(next)
}

func TestWorkQueue_RateLimited(t *testing.T) {
	q := NewWorkQueue(5*time.Millisecond, 50*time.Millisecond)

	// 直接测试限速器（不经过 Get 的 processing 保护，避免 dirty 重排逻辑干扰）。
	rl := NewItemExponentialFailureRateLimiter(5*time.Millisecond, 50*time.Millisecond)
	cases := []struct {
		attempt int
		minWait time.Duration
	}{
		{1, 5 * time.Millisecond},
		{2, 10 * time.Millisecond},
		{3, 20 * time.Millisecond},
		{4, 40 * time.Millisecond},
		{5, 50 * time.Millisecond}, // 触顶 maxDelay
	}
	for _, tc := range cases {
		delay := rl.When("x")
		if delay < tc.minWait {
			t.Fatalf("attempt %d: expected >= %v, got %v", tc.attempt, tc.minWait, delay)
		}
	}
	rl.Forget("x")
	if rl.NumRequeues("x") != 0 {
		t.Fatalf("expected forget to reset requeues")
	}
	_ = q
}

func TestWorkQueue_AddAfter(t *testing.T) {
	q := NewWorkQueue(time.Millisecond, time.Second)

	start := time.Now()
	q.AddAfter("delayed", 30*time.Millisecond)
	item, _ := q.Get()
	elapsed := time.Since(start)
	if elapsed < 25*time.Millisecond {
		t.Fatalf("expected delayed arrival, got %v", elapsed)
	}
	q.Done(item)
}

func TestWorkQueue_Shutdown(t *testing.T) {
	q := NewWorkQueue(time.Millisecond, time.Second)

	done := make(chan struct{})
	go func() {
		defer close(done)
		q.Get()
	}()

	time.Sleep(20 * time.Millisecond)
	q.ShutDown()

	select {
	case <-done:
		// OK
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("Get did not return after shutdown")
	}
}

func TestWorkQueue_ConcurrentAdd(t *testing.T) {
	q := NewWorkQueue(time.Millisecond, time.Second)

	// 并发 Add 1000 个 key，验证无死锁且去重正确
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			q.Add("task-a") // 全部相同 → 只应有 1 个
		}
		close(done)
	}()

	item, _ := q.Get()
	<-done
	if item != "task-a" {
		t.Fatalf("expected task-a")
	}
}

func TestWorkQueue_GetCtxCancellation(t *testing.T) {
	q := NewWorkQueue(time.Millisecond, time.Second)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		key, shutdown := q.GetCtx(ctx)
		if !shutdown {
			t.Errorf("expected shutdown on ctx cancel, got key=%q shutdown=%v", key, shutdown)
		}
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// OK：ctx 取消后 GetCtx 应返回
	case <-time.After(200 * time.Millisecond):
		t.Fatal("GetCtx did not return after context cancellation")
	}
}

func TestWorkQueue_GetCtxGetsItem(t *testing.T) {
	q := NewWorkQueue(time.Millisecond, time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	got := make(chan string, 1)
	go func() {
		key, shutdown := q.GetCtx(ctx)
		if !shutdown {
			got <- key
		}
	}()

	q.Add("task-x")
	select {
	case k := <-got:
		if k != "task-x" {
			t.Fatalf("expected task-x, got %q", k)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("GetCtx did not return queued item")
	}
}
