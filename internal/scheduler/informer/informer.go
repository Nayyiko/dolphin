package informer

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/yourname/dolphin/internal/pkg/model"
)

// TaskItem 本地缓存条目。
type TaskItem struct {
	Task   model.Task
	// Generation 变更版本（来自 updated_at 纳秒），用于乐观锁冲突检测。
	Generation int64
}

// TaskLister 只读查询接口（对标 k8s client-go listers）。
type TaskLister interface {
	List() []*TaskItem
	ListByStatus(status string) []*TaskItem
	Get(taskID string) (*TaskItem, bool)
}

// Store 任务本地缓存存储。
type Store struct {
	mu    sync.RWMutex
	items map[string]*TaskItem
}

func newStore() *Store {
	return &Store{items: make(map[string]*TaskItem)}
}

func (s *Store) List() []*TaskItem {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*TaskItem, 0, len(s.items))
	for _, it := range s.items {
		out = append(out, it)
	}
	return out
}

func (s *Store) ListByStatus(status string) []*TaskItem {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*TaskItem, 0)
	for _, it := range s.items {
		if it.Task.Status == status {
			out = append(out, it)
		}
	}
	return out
}

func (s *Store) Get(taskID string) (*TaskItem, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	it, ok := s.items[taskID]
	return it, ok
}

func (s *Store) Upsert(item *TaskItem) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[item.Task.ID] = item
}

func (s *Store) Delete(taskID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, taskID)
}

// EventType 变更事件类型。
type EventType int

const (
	EventAdd EventType = iota
	EventUpdate
	EventDelete
)

// Event 变更事件。
type Event struct {
	Type EventType
	Item *TaskItem
}

// Lister 从数据库获取任务列表的函数类型（解耦存储实现）。
// 返回所有任务的列表。
type Lister func(ctx context.Context) ([]model.Task, error)

// Watcher 增量获取变更的函数类型。
// 返回自 lastSync 之后更新的任务。
type Watcher func(ctx context.Context, lastSync time.Time) ([]model.Task, error)

// TaskInformer 任务 Informer。
// 对标 k8s informer：
//   - List: 启动时全量同步到本地缓存。
//   - Watch: 周期增量拉取（简化版：基于 updated_at 轮询，真实场景可接 etcd watch/binlog）。
//   - 事件分发: 变更后通过事件通道广播。
type TaskInformer struct {
	store      *Store
	lister     Lister
	watcher    Watcher
	pollInterval time.Duration

	events chan Event
	mu     sync.Mutex
	closed bool

	// 可注入的事件处理器
	handlers []EventHandler
}

// EventHandler 事件处理接口。
type EventHandler interface {
	OnAdd(item *TaskItem)
	OnUpdate(oldItem, newItem *TaskItem)
	OnDelete(item *TaskItem)
}

// NewTaskInformer 创建 Informer。
func NewTaskInformer(lister Lister, watcher Watcher, pollInterval time.Duration) *TaskInformer {
	return &TaskInformer{
		store:        newStore(),
		lister:       lister,
		watcher:      watcher,
		pollInterval: pollInterval,
		events:       make(chan Event, 256),
	}
}

// AddEventHandler 注册事件处理器。
func (ti *TaskInformer) AddEventHandler(h EventHandler) {
	ti.mu.Lock()
	defer ti.mu.Unlock()
	ti.handlers = append(ti.handlers, h)
}

// GetLister 返回只读查询接口。
func (ti *TaskInformer) GetLister() TaskLister {
	return ti.store
}

// Start 启动 Informer：先 List 全量同步，再后台 Watch 增量。
func (ti *TaskInformer) Start(ctx context.Context) error {
	// 1. List 全量同步
	tasks, err := ti.lister(ctx)
	if err != nil {
		return err
	}
	for i := range tasks {
		t := tasks[i]
		ti.store.Upsert(&TaskItem{Task: t, Generation: t.UpdatedAt.UnixNano()})
	}
	slog.Info("informer: initial list synced", "count", len(tasks))

	// 2. 后台 Watch
	go ti.watchLoop(ctx)
	go ti.dispatchLoop(ctx)
	return nil
}

// watchLoop 增量轮询。记录最后同步时间，周期性拉取变更。
func (ti *TaskInformer) watchLoop(ctx context.Context) {
	lastSync := time.Now()
	ticker := time.NewTicker(ti.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			changed, err := ti.watcher(ctx, lastSync)
			if err != nil {
				slog.Error("informer: watch failed", "err", err)
				continue
			}
			lastSync = time.Now()
			ti.applyChanges(changed)
		}
	}
}

// applyChanges 将增量变更应用到缓存并广播事件。
func (ti *TaskInformer) applyChanges(changed []model.Task) {
	for i := range changed {
		t := changed[i]
		gen := t.UpdatedAt.UnixNano()

		_, existed := ti.store.Get(t.ID)
		if t.Status == model.TaskStatusDeleted {
			// 软删除状态 → 从缓存移除
			if existed {
				ti.store.Delete(t.ID)
				item := &TaskItem{Task: t, Generation: gen}
				ti.broadcast(Event{Type: EventDelete, Item: item})
			}
			continue
		}

		item := &TaskItem{Task: t, Generation: gen}
		if !existed {
			ti.store.Upsert(item)
			ti.broadcast(Event{Type: EventAdd, Item: item})
		} else {
			ti.store.Upsert(item)
			ti.broadcast(Event{Type: EventUpdate, Item: item})
		}
	}
}

// broadcast 分发事件给所有 handler。
func (ti *TaskInformer) broadcast(ev Event) {
	ti.mu.Lock()
	handlers := make([]EventHandler, len(ti.handlers))
	copy(handlers, ti.handlers)
	ti.mu.Unlock()

	for _, h := range handlers {
		switch ev.Type {
		case EventAdd:
			h.OnAdd(ev.Item)
		case EventUpdate:
			h.OnUpdate(nil, ev.Item)
		case EventDelete:
			h.OnDelete(ev.Item)
		}
	}
}

// dispatchLoop 从事件通道读取并分发（供需要异步消费的场景）。
func (ti *TaskInformer) dispatchLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-ti.events:
			ti.broadcast(ev)
		}
	}
}

// Stop 停止 Informer。
func (ti *TaskInformer) Stop() {
	ti.mu.Lock()
	ti.closed = true
	ti.mu.Unlock()
}
