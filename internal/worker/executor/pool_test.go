package executor

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type mockReporter struct {
	mu      sync.Mutex
	results []*TaskResult
}

func (m *mockReporter) ReportResult(ctx context.Context, r *TaskResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.results = append(m.results, r)
	return nil
}

func (m *mockReporter) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.results)
}

func (m *mockReporter) last() *TaskResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.results) == 0 {
		return nil
	}
	return m.results[len(m.results)-1]
}

func TestPool_SubmitAndExecute(t *testing.T) {
	// 测试 HTTP 任务执行
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	}))
	defer ts.Close()

	reporter := &mockReporter{}
	pool := NewPool(2, reporter)
	defer pool.Shutdown()

	task := &TaskDispatch{
		TaskID:      "t1",
		InstanceID:  "inst-1",
		Handler:     ts.URL,
		HandlerType: "http",
		Timeout:     5,
	}
	if !pool.Submit(task) {
		t.Fatalf("submit should succeed")
	}

	// 等待结果上报
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if reporter.count() >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if reporter.count() != 1 {
		t.Fatalf("expected 1 result, got %d", reporter.count())
	}
	r := reporter.last()
	if r.Status != "success" || r.Result != "ok" {
		t.Fatalf("expected success/ok, got %s/%s", r.Status, r.Result)
	}
}

func TestPool_Timeout(t *testing.T) {
	// 测试超时：任务睡眠超过 timeout 应报 timeout
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		_, _ = w.Write([]byte("late"))
	}))
	defer ts.Close()

	reporter := &mockReporter{}
	pool := NewPool(2, reporter)
	defer pool.Shutdown()

	task := &TaskDispatch{
		TaskID:      "t2",
		InstanceID:  "inst-2",
		Handler:     ts.URL,
		HandlerType: "http",
		Timeout:     1, // 1 秒超时
	}
	pool.Submit(task)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if reporter.count() >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if reporter.count() != 1 {
		t.Fatalf("expected 1 result, got %d", reporter.count())
	}
	r := reporter.last()
	if r.Status != "timeout" {
		t.Fatalf("expected timeout, got %s", r.Status)
	}
}

func TestPool_Full(t *testing.T) {
	// 队列满时 Submit 应返回 false
	reporter := &mockReporter{}
	pool := NewPool(1, reporter) // capacity=1, buffer=2

	// 阻塞 worker 的任务
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
	}))
	defer ts.Close()

	for i := 0; i < 5; i++ {
		task := &TaskDispatch{
			TaskID:      "t",
			InstanceID:  "inst-" + itoa(i),
			Handler:     ts.URL,
			HandlerType: "http",
			Timeout:     5,
		}
		pool.Submit(task)
	}

	// 队列应该已满，新的提交被拒绝
	task := &TaskDispatch{
		TaskID: "t", InstanceID: "inst-overflow", Handler: "http://x",
		HandlerType: "http", Timeout: 5,
	}
	if pool.Submit(task) {
		t.Fatalf("expected submit to fail when pool full")
	}
}

func itoa(i int) string {
	return fmt.Sprintf("%d", i)
}
