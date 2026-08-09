package informer

import (
	"context"
	"testing"
	"time"

	"github.com/yourname/dolphin/internal/pkg/model"
)

type fakeHandler struct {
	adds    []string
	updates []string
	deletes []string
}

func (f *fakeHandler) OnAdd(item *TaskItem)  { f.adds = append(f.adds, item.Task.ID) }
func (f *fakeHandler) OnUpdate(_, newItem *TaskItem) { f.updates = append(f.updates, newItem.Task.ID) }
func (f *fakeHandler) OnDelete(item *TaskItem) { f.deletes = append(f.deletes, item.Task.ID) }

func mkTask(id string, updated time.Time) model.Task {
	return model.Task{
		ID:        id,
		Name:      "task-" + id,
		Status:    model.TaskStatusActive,
		UpdatedAt: updated,
	}
}

func TestInformer_ListSync(t *testing.T) {
	base := time.Now()
	lister := func(ctx context.Context) ([]model.Task, error) {
		return []model.Task{
			mkTask("t1", base),
			mkTask("t2", base),
		}, nil
	}
	watcher := func(ctx context.Context, since time.Time) ([]model.Task, error) {
		return nil, nil
	}

	inf := NewTaskInformer(lister, watcher, time.Hour)
	if err := inf.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	items := inf.GetLister().List()
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
	if _, ok := inf.GetLister().Get("t1"); !ok {
		t.Fatalf("t1 not in store")
	}
}

func TestInformer_ApplyChanges(t *testing.T) {
	base := time.Now()
	inf := NewTaskInformer(
		func(ctx context.Context) ([]model.Task, error) { return nil, nil },
		func(ctx context.Context, since time.Time) ([]model.Task, error) { return nil, nil },
		time.Hour,
	)

	h := &fakeHandler{}
	inf.AddEventHandler(h)

	// 新增
	inf.applyChanges([]model.Task{mkTask("t1", base)})
	if _, ok := inf.GetLister().Get("t1"); !ok {
		t.Fatalf("t1 not added to store")
	}
	if len(h.adds) != 1 {
		t.Fatalf("expected 1 add event, got %d", len(h.adds))
	}

	// 更新
	updated := base.Add(time.Minute)
	t1 := mkTask("t1", updated)
	t1.Name = "renamed"
	inf.applyChanges([]model.Task{t1})
	item, ok := inf.GetLister().Get("t1")
	if !ok {
		t.Fatalf("t1 missing after update")
	}
	if item.Task.Name != "renamed" {
		t.Fatalf("expected renamed, got %q", item.Task.Name)
	}
	if len(h.updates) != 1 {
		t.Fatalf("expected 1 update event, got %d", len(h.updates))
	}

	// 删除
	deleted := mkTask("t1", updated)
	deleted.Status = model.TaskStatusDeleted
	inf.applyChanges([]model.Task{deleted})
	if _, ok := inf.GetLister().Get("t1"); ok {
		t.Fatalf("t1 should be removed from store")
	}
	if len(h.deletes) != 1 {
		t.Fatalf("expected 1 delete event, got %d", len(h.deletes))
	}
}

func TestInformer_StoreStatusFilter(t *testing.T) {
	s := newStore()
	base := time.Now()
	s.Upsert(&TaskItem{Task: mkTask("a", base), Generation: 1})
	paused := mkTask("b", base)
	paused.Status = model.TaskStatusPaused
	s.Upsert(&TaskItem{Task: paused, Generation: 1})

	active := s.ListByStatus(model.TaskStatusActive)
	if len(active) != 1 || active[0].Task.ID != "a" {
		t.Fatalf("expected only a active, got %+v", active)
	}
}
