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
