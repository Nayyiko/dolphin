package executor

import (
	"testing"

	"github.com/yourname/dolphin/internal/pkg/model"
)

// TestLeastLoadedSelectsByRatio 选负载比（load/max_concurrency）最小的 Worker。
// 关键场景：A 负载 5/10（0.5） vs B 负载 20/50（0.4）→ 选 B。
// 若按绝对值排序会选 A，这是本用例要防住的回归。
func TestLeastLoadedSelectsByRatio(t *testing.T) {
	workers := []model.Worker{
		{ID: "big-heavy", MaxConcurrency: 50, CurrentLoad: 20}, // ratio 0.40
		{ID: "small-mid", MaxConcurrency: 10, CurrentLoad: 5},  // ratio 0.50
		{ID: "idle", MaxConcurrency: 10, CurrentLoad: 0},       // ratio 0.00
	}
	got := LeastLoadedSelector{}.Select(workers)
	if got == nil || got.ID != "idle" {
		t.Fatalf("expected idle worker, got %+v", got)
	}
}

// TestLeastLoadedEmpty 无 Worker 返回 nil。
func TestLeastLoadedEmpty(t *testing.T) {
	got := LeastLoadedSelector{}.Select(nil)
	if got != nil {
		t.Fatalf("expected nil, got %+v", got)
	}
}

// TestLeastLoadedTies 同负载比时选第一个（稳定）。
func TestLeastLoadedTies(t *testing.T) {
	workers := []model.Worker{
		{ID: "a", MaxConcurrency: 10, CurrentLoad: 2},
		{ID: "b", MaxConcurrency: 50, CurrentLoad: 10},
	}
	got := LeastLoadedSelector{}.Select(workers)
	if got == nil || got.ID != "a" {
		t.Fatalf("expected a on tie, got %+v", got)
	}
}

// TestLeastLoadedMaxConcurrencyZero 缺失 max_concurrency（0）时退化为绝对值比较，不除零崩溃。
func TestLeastLoadedMaxConcurrencyZero(t *testing.T) {
	workers := []model.Worker{
		{ID: "a", CurrentLoad: 3},
		{ID: "b", CurrentLoad: 1},
	}
	got := LeastLoadedSelector{}.Select(workers)
	if got == nil || got.ID != "b" {
		t.Fatalf("expected b (lower absolute load), got %+v", got)
	}
}

// TestRoundRobin 轮询选择循环返回。
func TestRoundRobin(t *testing.T) {
	workers := []model.Worker{{ID: "a"}, {ID: "b"}}
	sel := &RoundRobinSelector{}
	seen := map[string]int{}
	for i := 0; i < 4; i++ {
		w := sel.Select(workers)
		seen[w.ID]++
	}
	if seen["a"] != 2 || seen["b"] != 2 {
		t.Fatalf("expected even round-robin, got %v", seen)
	}
}
