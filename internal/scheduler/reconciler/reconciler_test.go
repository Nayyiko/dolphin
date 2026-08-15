package reconciler

import (
	"testing"
	"time"

	"github.com/yourname/dolphin/internal/pkg/model"
	"github.com/yourname/dolphin/internal/scheduler/dag"
	"github.com/yourname/dolphin/internal/scheduler/informer"
	"github.com/yourname/dolphin/internal/scheduler/queue"
)

// fakeLister 内存版 TaskLister，用于不依赖 DB 的单元测试。
type fakeLister struct {
	items []*informer.TaskItem
}

func (f *fakeLister) List() []*informer.TaskItem                      { return f.items }
func (f *fakeLister) ListByStatus(status string) []*informer.TaskItem { return f.items }
func (f *fakeLister) Get(taskID string) (*informer.TaskItem, bool) {
	for _, it := range f.items {
		if it.Task.ID == taskID {
			return it, true
		}
	}
	return nil, false
}
func (f *fakeLister) UpdateNextRunAt(taskID string, t time.Time) {}

// drain 从队列取出 n 个 key；超时未取到则返回已取到的部分（防止测试挂死）。
func drain(q *queue.WorkQueue, n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		type res struct {
			id string
		}
		ch := make(chan res, 1)
		go func() {
			id, _ := q.Get()
			ch <- res{id}
		}()
		select {
		case r := <-ch:
			out = append(out, r.id)
		case <-time.After(2 * time.Second):
			return out
		}
	}
	return out
}

// TestEnqueueDependents 上游完成 → 只推送直接依赖它的 active 下游。
func TestEnqueueDependents(t *testing.T) {
	q := queue.NewWorkQueue(time.Second, time.Minute)
	items := []*informer.TaskItem{
		{Task: model.Task{ID: "b", Status: model.TaskStatusActive, DependOn: dag.MarshalDependOn([]string{"a"})}},
		{Task: model.Task{ID: "c", Status: model.TaskStatusActive, DependOn: dag.MarshalDependOn([]string{"a", "b"})}},
		{Task: model.Task{ID: "d", Status: model.TaskStatusActive}}, // 无依赖
		{Task: model.Task{ID: "e", Status: model.TaskStatusPaused, DependOn: dag.MarshalDependOn([]string{"a"})}}, // 暂停
	}
	r := &Reconciler{queue: q, lister: &fakeLister{items: items}}

	r.EnqueueDependents("a")
	got := drain(q, 2)
	if len(got) != 2 {
		t.Fatalf("expected 2 downstream enqueued, got %v", got)
	}
	for _, id := range got {
		if id != "b" && id != "c" {
			t.Fatalf("unexpected downstream %q", id)
		}
	}
}

// TestEnqueueDependentsNoMatch 上游没有下游 → 不入队。
func TestEnqueueDependentsNoMatch(t *testing.T) {
	q := queue.NewWorkQueue(time.Second, time.Minute)
	items := []*informer.TaskItem{
		{Task: model.Task{ID: "b", Status: model.TaskStatusActive, DependOn: dag.MarshalDependOn([]string{"a"})}},
	}
	r := &Reconciler{queue: q, lister: &fakeLister{items: items}}

	r.EnqueueDependents("ghost")
	if q.Len() != 0 {
		t.Fatalf("expected nothing enqueued, queue len = %d", q.Len())
	}
}

// ---------- 执行级重试：纯决策函数 ----------

// TestRetryDecision 覆盖重试决策核心：
// 禁用、首次/多次重试的编号与指数退避、耗尽判定。
func TestRetryDecision(t *testing.T) {
	cases := []struct {
		name        string
		last, max   int
		base        time.Duration
		wantOK      bool
		wantAttempt int
		wantDelay   time.Duration
	}{
		{"disabled max=0", 0, 0, 2 * time.Second, false, 0, 0},
		{"first retry", 0, 3, 2 * time.Second, true, 1, 2 * time.Second},
		{"second retry", 1, 3, 2 * time.Second, true, 2, 4 * time.Second},
		{"third retry", 2, 3, 2 * time.Second, true, 3, 8 * time.Second},
		{"exhausted", 3, 3, 2 * time.Second, false, 0, 0},
		{"exhausted attempt>max", 2, 2, 2 * time.Second, false, 0, 0},
		{"default base when 0", 0, 2, 0, true, 1, defaultRetryBaseDelay},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			attempt, delay, ok := retryDecision(c.last, c.max, c.base)
			if ok != c.wantOK {
				t.Fatalf("ok = %v, want %v", ok, c.wantOK)
			}
			if attempt != c.wantAttempt {
				t.Fatalf("attempt = %d, want %d", attempt, c.wantAttempt)
			}
			if delay != c.wantDelay {
				t.Fatalf("delay = %v, want %v", delay, c.wantDelay)
			}
		})
	}
}

// TestRetryDecisionOverflowCapped 极端重试次数下退避时长应被钳制，防止溢出。
func TestRetryDecisionOverflowCapped(t *testing.T) {
	_, delay, ok := retryDecision(32, 100, 2*time.Second)
	if !ok {
		t.Fatal("expected ok=true")
	}
	want := 2 * time.Second << 30
	if delay != want {
		t.Fatalf("delay = %v, want %v (capped at 2^30)", delay, want)
	}
}

// TestRetryBackoff 方法版退避：n 从 0 起，base * 2^n，超限钳制到 2^30。
func TestRetryBackoff(t *testing.T) {
	r := &Reconciler{retryBaseDelay: 0} // 0 → 触发默认 base
	cases := []struct {
		n    int
		want time.Duration
	}{
		{0, 2 * time.Second},
		{1, 4 * time.Second},
		{2, 8 * time.Second},
		{40, 2 * time.Second << 30},
	}
	for _, c := range cases {
		if got := r.retryBackoff(c.n); got != c.want {
			t.Fatalf("retryBackoff(%d) = %v, want %v", c.n, got, c.want)
		}
	}
}

// TestScheduleRetryNonLeaderNoop 非 Leader（leaderCtx 为 nil）时不安排重试，也不留下去重标记。
func TestScheduleRetryNonLeaderNoop(t *testing.T) {
	r := &Reconciler{
		retrying:        make(map[string]bool),
		dispatchFailures: make(map[string]int),
	}
	r.scheduleRetry("t1") // 不 panic，直接返回
	if r.retrying["t1"] {
		t.Fatal("non-leader should not mark retrying")
	}
}

// TestOnTaskResultSuccessNoRetry 成功结果不触发重试；无依赖时不入队。
func TestOnTaskResultSuccessNoRetry(t *testing.T) {
	q := queue.NewWorkQueue(time.Second, time.Minute)
	r := &Reconciler{
		queue:           q,
		lister:          &fakeLister{},
		retrying:        make(map[string]bool),
		dispatchFailures: make(map[string]int),
	}
	r.OnTaskResult("a", model.TaskLogStatusSuccess)
	if q.Len() != 0 {
		t.Fatalf("expected no enqueue, got %d", q.Len())
	}
	if r.retrying["a"] {
		t.Fatal("success result should not schedule retry")
	}
}

// TestOnTaskResultFailedNonLeaderNoRetry 失败结果在非 Leader 时也不标记重试（failover 安全）。
func TestOnTaskResultFailedNonLeaderNoRetry(t *testing.T) {
	r := &Reconciler{
		lister:          &fakeLister{},
		retrying:        make(map[string]bool),
		dispatchFailures: make(map[string]int),
	}
	r.OnTaskResult("a", model.TaskLogStatusFailed)
	if r.retrying["a"] {
		t.Fatal("non-leader should not mark retrying")
	}
}
