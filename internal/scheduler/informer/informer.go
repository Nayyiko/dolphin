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
	// UpdateNextRunAt 直接更新本地缓存中的 next_run_at。
	// 用于 Reconciler 下发后同步缓存，避免 Informer 轮询延迟导致重复调度。
	UpdateNextRunAt(taskID string, t time.Time)
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

// UpdateNextRunAt 直接更新本地缓存中任务的 next_run_at。
// 避免 Reconciler dispatch 后 Informer 下一个轮询周期前，
// 到期扫描器仍看到旧的过期时间导致重复调度。
func (s *Store) UpdateNextRunAt(taskID string, t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if it, ok := s.items[taskID]; ok {
		it.Task.NextRunAt = t
	}
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

// watchLookback 增量同步水位的回退窗口。
//
// updated_at 由 gorm 在「构建 INSERT/UPDATE 语句」时用 Go 时钟赋值，而行的可见性要
// 等到「提交」才发生，二者之间隔着一次 MySQL 往返（本环境 ~20-50ms，负载下可上百 ms）。
// 若水位只推进到 max(updated_at)，一个「构建早、提交晚」的行会被下一轮 `updated_at > lastSync`
// 永久漏掉。回退一个足够大的窗口（1s，>10x 余量）把这类乱序提交的行兜回来；
// 重读是幂等 upsert，reconcile 对 next_run_at 已推进的任务是 no-op，无副作用。
const watchLookback = time.Second

// maxTaskUpdatedAt 返回切片中最大的 updated_at（空切片返回零值）。
func maxTaskUpdatedAt(tasks []model.Task) time.Time {
	var maxUpdated time.Time
	for i := range tasks {
		if tasks[i].UpdatedAt.After(maxUpdated) {
			maxUpdated = tasks[i].UpdatedAt
		}
	}
	return maxUpdated
}

// advanceSyncTime 计算下一轮轮询的同步水位。
//
// 关键：水位必须推进到「本次返回行的最大 updated_at - watchLookback」，而不是 time.Now()。
// 若用 time.Now()：某行 INSERT 在 SELECT 快照之后才提交，其 updated_at 早于本轮 poll
// 结束时的 time.Now()，下一轮 `updated_at > lastSync` 就永久漏掉它 → 任务进不了本地缓存，
// reconcile 每次 lister.Get 落空直接 return，任务被静默丢弃。
// （场景 C 实测 170 个任务 create 时丢 3 个，正是此竞态。）
// 空结果时不推进水位（推进到 now 同样会跳过「快照后、now 前」才提交的行）。
// 水位只前进不回退。
func advanceSyncTime(prev time.Time, changed []model.Task) time.Time {
	maxUpdated := maxTaskUpdatedAt(changed)
	if maxUpdated.IsZero() {
		return prev
	}
	next := maxUpdated.Add(-watchLookback)
	if next.After(prev) {
		return next
	}
	return prev
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

	// 2. 后台 Watch。初始水位 = List 返回行的最大 updated_at 回退 watchLookback，
	// 封住「List 快照之后、Watch 启动之前才提交的新行」的 List-Watch 间隙。
	// （不能用 time.Now()，理由同 advanceSyncTime。）
	go ti.watchLoop(ctx, advanceSyncTime(time.Time{}, tasks))
	go ti.dispatchLoop(ctx)
	return nil
}

// watchLoop 增量轮询。记录最后同步时间，周期性拉取变更。
func (ti *TaskInformer) watchLoop(ctx context.Context, lastSync time.Time) {
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
			lastSync = advanceSyncTime(lastSync, changed)
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
