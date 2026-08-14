package queue

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// RateLimiter 限速接口。决定一个 key 失败后多久才能重新入队。
type RateLimiter interface {
	// When 返回 item 应该等待多久才能再次入队。
	When(item string) time.Duration
	// Forget 清除该 item 的限速记录（成功后调用）。
	Forget(item string)
	// NumRequeues 返回该 item 被重新入队的次数。
	NumRequeues(item string) int
}

// ItemExponentialFailureRateLimiter 逐 key 指数退避限速器。
// baseDelay * 2^failures，上限 maxDelay。
type ItemExponentialFailureRateLimiter struct {
	failures  map[string]int
	baseDelay time.Duration
	maxDelay  time.Duration
	mu        sync.Mutex
}

// NewItemExponentialFailureRateLimiter 创建指数退避限速器。
func NewItemExponentialFailureRateLimiter(baseDelay, maxDelay time.Duration) *ItemExponentialFailureRateLimiter {
	return &ItemExponentialFailureRateLimiter{
		failures:  make(map[string]int),
		baseDelay: baseDelay,
		maxDelay:  maxDelay,
	}
}

func (r *ItemExponentialFailureRateLimiter) When(item string) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()

	exp := r.failures[item]
	r.failures[item] = exp + 1

	delay := r.baseDelay * time.Duration(1<<uint(exp))
	if delay > r.maxDelay {
		delay = r.maxDelay
	}
	return delay
}

func (r *ItemExponentialFailureRateLimiter) Forget(item string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.failures, item)
}

func (r *ItemExponentialFailureRateLimiter) NumRequeues(item string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failures[item]
}

// BucketRateLimiter 全局令牌桶限速器（可选，用于整体限流而非逐 key）。
type BucketRateLimiter struct {
	limiter *rate.Limiter
}

// NewBucketRateLimiter 创建全局令牌桶。
func NewBucketRateLimiter(qps int) *BucketRateLimiter {
	return &BucketRateLimiter{limiter: rate.NewLimiter(rate.Limit(qps), qps)}
}

func (r *BucketRateLimiter) When(item string) time.Duration {
	return r.limiter.Reserve().Delay()
}

func (r *BucketRateLimiter) Forget(item string) {}

func (r *BucketRateLimiter) NumRequeues(item string) int {
	return 0
}

// WorkQueue 限速工作队列。
// 对标 k8s.io/client-go/util/workqueue。
//
// 设计要点:
//   - dirty set: 已在队列中的 key 去重，避免同一任务重复排队。
//   - processing set: 正在被处理的 key，防止并发处理同一任务。
//   - RateLimiter: 失败的任务按指数退避延迟重新入队。
type WorkQueue struct {
	queue      []string
	dirty      map[string]bool
	processing map[string]bool

	rateLimiter RateLimiter

	cond     *sync.Cond
	shutdown bool
}

// NewWorkQueue 创建带指数退避限速的工作队列。
func NewWorkQueue(baseDelay, maxDelay time.Duration) *WorkQueue {
	return &WorkQueue{
		queue:       make([]string, 0),
		dirty:       make(map[string]bool),
		processing:  make(map[string]bool),
		rateLimiter: NewItemExponentialFailureRateLimiter(baseDelay, maxDelay),
		cond:        sync.NewCond(&sync.Mutex{}),
	}
}

// NewWorkQueueWithRateLimiter 使用自定义限速器。
func NewWorkQueueWithRateLimiter(rl RateLimiter) *WorkQueue {
	return &WorkQueue{
		queue:       make([]string, 0),
		dirty:       make(map[string]bool),
		processing:  make(map[string]bool),
		rateLimiter: rl,
		cond:        sync.NewCond(&sync.Mutex{}),
	}
}

// Add 将 key 加入队列（去重）。
func (q *WorkQueue) Add(item string) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	if q.shutdown {
		return
	}
	if q.dirty[item] {
		return // 已在队列中
	}
	q.dirty[item] = true
	if q.processing[item] {
		return // 正在处理，处理完成后会因 dirty=true 自动重排
	}
	q.queue = append(q.queue, item)
	q.cond.Signal()
}

// AddRateLimited 限速入队（失败后调用）。
func (q *WorkQueue) AddRateLimited(item string) {
	delay := q.rateLimiter.When(item)
	time.AfterFunc(delay, func() {
		q.Add(item)
	})
}

// AddAfter 延迟入队（用于 cron 预计算：到期时间到了再入队）。
func (q *WorkQueue) AddAfter(item string, duration time.Duration) {
	if duration <= 0 {
		q.Add(item)
		return
	}
	time.AfterFunc(duration, func() {
		q.Add(item)
	})
}

// Get 阻塞获取下一个待处理 key。返回 (key, shutdown)。
func (q *WorkQueue) Get() (string, bool) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	for len(q.queue) == 0 && !q.shutdown {
		q.cond.Wait()
	}
	if q.shutdown {
		return "", true
	}

	item := q.queue[0]
	q.queue = q.queue[1:]

	q.processing[item] = true
	delete(q.dirty, item)

	return item, false
}

// GetCtx 类似 Get，但 ctx 取消（或队列 ShutDown）时解除阻塞并返回 shutdown=true。
// 用于 Leader 失权时让 reconciler worker 真正退出，而不是空等队列。
func (q *WorkQueue) GetCtx(ctx context.Context) (string, bool) {
	// ctx 取消时唤醒 cond 等待者；GetCtx 返回后停止 goroutine。
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			q.cond.Broadcast()
		case <-stop:
		}
	}()

	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	for len(q.queue) == 0 && !q.shutdown && ctx.Err() == nil {
		q.cond.Wait()
	}
	if q.shutdown || ctx.Err() != nil {
		return "", true
	}

	item := q.queue[0]
	q.queue = q.queue[1:]

	q.processing[item] = true
	delete(q.dirty, item)

	return item, false
}

// Done 标记处理完成。
// 若处理期间有人重新 Add 了该 key（dirty=true），自动重新入队。
// 注意：Done 不重置限速器；成功时由调用方显式调用 Forget。
func (q *WorkQueue) Done(item string) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	delete(q.processing, item)

	if q.dirty[item] {
		q.queue = append(q.queue, item)
		delete(q.dirty, item)
		q.cond.Signal()
	}
}

// Forget 清除该 key 的限速记录（成功后调用）。
func (q *WorkQueue) Forget(item string) {
	q.rateLimiter.Forget(item)
}

// NumRequeues 返回某个 key 的重试次数。
func (q *WorkQueue) NumRequeues(item string) int {
	return q.rateLimiter.NumRequeues(item)
}

// Len 返回队列中待处理的数量。
func (q *WorkQueue) Len() int {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	return len(q.queue)
}

// ShutDown 关闭队列，唤醒所有等待的 Get。
func (q *WorkQueue) ShutDown() {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.shutdown = true
	q.cond.Broadcast()
}
