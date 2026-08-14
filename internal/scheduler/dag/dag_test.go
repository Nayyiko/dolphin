package dag

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/yourname/dolphin/internal/pkg/model"
)

func task(id, dependOn, policy string) model.Task {
	return model.Task{ID: id, DependOn: dependOn, DepPolicy: policy}
}

// TestParseMarshalDependOn JSON 序列化往返。
func TestParseMarshalDependOn(t *testing.T) {
	if s := MarshalDependOn(nil); s != "" {
		t.Fatalf("MarshalDependOn(nil) = %q, want empty", s)
	}
	if s := MarshalDependOn([]string{}); s != "" {
		t.Fatalf("MarshalDependOn(empty) = %q, want empty", s)
	}
	s := MarshalDependOn([]string{"a", "b"})
	if s != `["a","b"]` {
		t.Fatalf("MarshalDependOn = %q", s)
	}
	got, err := ParseDependOn(s)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("ParseDependOn = %v", got)
	}
	if _, err := ParseDependOn(`{bad`); err == nil {
		t.Fatal("expected parse error for malformed JSON")
	}
}

// TestGraphBasics 建图 + 上下游查询。
func TestGraphBasics(t *testing.T) {
	g, err := NewGraph([]model.Task{
		task("a", "", ""),
		task("b", MarshalDependOn([]string{"a"}), model.DepPolicyAllSuccess),
		task("c", MarshalDependOn([]string{"a", "b"}), model.DepPolicyAllSuccess),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := g.DependentsOf("a"); len(got) != 2 || !contains(got, "b") || !contains(got, "c") {
		t.Fatalf("DependentsOf(a) = %v", got)
	}
	if got := g.DependenciesOf("c"); len(got) != 2 {
		t.Fatalf("DependenciesOf(c) = %v", got)
	}
	order, err := g.TopoOrder()
	if err != nil {
		t.Fatal(err)
	}
	if len(order) != 3 {
		t.Fatalf("topo order length = %d, want 3", len(order))
	}
	// 拓扑序约束：a 必须在 b 和 c 之前，b 必须在 c 之前。
	pos := map[string]int{}
	for i, id := range order {
		pos[id] = i
	}
	if !(pos["a"] < pos["b"] && pos["b"] < pos["c"] && pos["a"] < pos["c"]) {
		t.Fatalf("invalid topo order: %v", order)
	}
}

// TestHasCycle 环检测返回具体路径。
func TestHasCycle(t *testing.T) {
	// a → b → c → a
	g, err := NewGraph([]model.Task{
		task("a", MarshalDependOn([]string{"c"}), model.DepPolicyAllSuccess),
		task("b", MarshalDependOn([]string{"a"}), model.DepPolicyAllSuccess),
		task("c", MarshalDependOn([]string{"b"}), model.DepPolicyAllSuccess),
	})
	if err != nil {
		t.Fatal(err)
	}
	path, ok := g.HasCycle()
	if !ok {
		t.Fatal("expected cycle detected")
	}
	// 路径应闭合且覆盖环上节点。
	if len(path) < 4 {
		t.Fatalf("cycle path too short: %v", path)
	}
	if path[0] != path[len(path)-1] {
		t.Fatalf("cycle path not closed: %v", path)
	}
	if _, err := g.TopoOrder(); err == nil {
		t.Fatal("expected TopoOrder to reject cyclic graph")
	} else if !errors.Is(err, ErrCycle) {
		t.Fatalf("expected ErrCycle, got %v", err)
	}
}

// TestSelfDependencyRejected 自依赖直接拒绝。
func TestSelfDependencyRejected(t *testing.T) {
	_, err := NewGraph([]model.Task{
		task("a", MarshalDependOn([]string{"a"}), model.DepPolicyAllSuccess),
	})
	if err == nil {
		t.Fatal("expected self-dependency rejected")
	}
}

// TestInvalidPolicyRejected 非法依赖策略拒绝。
func TestInvalidPolicyRejected(t *testing.T) {
	_, err := NewGraph([]model.Task{
		task("a", MarshalDependOn([]string{"b"}), "any_of"),
	})
	if err == nil {
		t.Fatal("expected invalid policy rejected")
	} else if !errors.Is(err, ErrInvalidDepPolicy) {
		t.Fatalf("expected ErrInvalidDepPolicy, got %v", err)
	}
}

// TestValidateDependOn 创建/更新时的图校验。
func TestValidateDependOn(t *testing.T) {
	all := []model.Task{
		task("a", "", ""),
		task("b", "", ""),
	}
	// 合法：c 依赖 a、b，无环。
	c := task("c", MarshalDependOn([]string{"a", "b"}), model.DepPolicyAllSuccess)
	if err := ValidateDependOn(all, &c); err != nil {
		t.Fatalf("expected valid, got %v", err)
	}
	// 无依赖 → 直接合法。
	noDep := task("d", "", "")
	if err := ValidateDependOn(all, &noDep); err != nil {
		t.Fatalf("no-dep should be valid: %v", err)
	}
	// 悬空引用：依赖不存在的任务。
	dangling := task("c", MarshalDependOn([]string{"ghost"}), model.DepPolicyAllSuccess)
	if err := ValidateDependOn(all, &dangling); err == nil {
		t.Fatal("expected dangling dependency rejected")
	} else if !errors.Is(err, ErrDependencyNotFound) {
		t.Fatalf("expected ErrDependencyNotFound, got %v", err)
	}
	// 成环：b 依赖 c（c 依赖 a、b 中已有 a），若 b 依赖 c 则 b→c→b。
	cycle := task("c", MarshalDependOn([]string{"b"}), model.DepPolicyAllSuccess)
	b := task("b", MarshalDependOn([]string{"c"}), model.DepPolicyAllSuccess)
	_ = b
	allWithB := append(all, b)
	if err := ValidateDependOn(allWithB, &cycle); err == nil {
		t.Fatal("expected cycle rejected")
	} else if !errors.Is(err, ErrCycle) {
		t.Fatalf("expected ErrCycle, got %v", err)
	}
}

// TestDepsSatisfied 运行时门控纯函数。
func TestDepsSatisfied(t *testing.T) {
	now := time.Now()
	base := now.Add(-2 * time.Minute)
	recent := now.Add(-time.Minute)

	lookup := func(logs map[string]*model.TaskLog) LogLookup {
		return func(taskID string, after time.Time) *model.TaskLog {
			l := logs[taskID]
			if l == nil || l.StartTime.Before(after) {
				return nil
			}
			return l
		}
	}

	log := func(status string, start time.Time) *model.TaskLog {
		return &model.TaskLog{TaskID: "u", Status: status, StartTime: start}
	}

	t.Run("all_success satisfied", func(t *testing.T) {
		logs := map[string]*model.TaskLog{"u": log(model.TaskLogStatusSuccess, recent)}
		ok, _ := DepsSatisfied([]string{"u"}, model.DepPolicyAllSuccess, base, lookup(logs))
		if !ok {
			t.Fatal("want satisfied")
		}
	})

	t.Run("all_success blocks stale run", func(t *testing.T) {
		// 上游成功在 base 之前 → 串到旧结果，必须阻塞。
		logs := map[string]*model.TaskLog{"u": log(model.TaskLogStatusSuccess, base)}
		ok, reason := DepsSatisfied([]string{"u"}, model.DepPolicyAllSuccess, recent, lookup(logs))
		if ok {
			t.Fatal("want blocked for stale upstream")
		}
		if !strings.Contains(reason, "u") {
			t.Fatalf("reason should name upstream: %s", reason)
		}
	})

	t.Run("all_success blocks failure", func(t *testing.T) {
		logs := map[string]*model.TaskLog{"u": log(model.TaskLogStatusFailed, recent)}
		ok, _ := DepsSatisfied([]string{"u"}, model.DepPolicyAllSuccess, base, lookup(logs))
		if ok {
			t.Fatal("want blocked for failed upstream")
		}
	})

	t.Run("all_completed accepts failure", func(t *testing.T) {
		logs := map[string]*model.TaskLog{"u": log(model.TaskLogStatusFailed, recent)}
		ok, _ := DepsSatisfied([]string{"u"}, model.DepPolicyAllCompleted, base, lookup(logs))
		if !ok {
			t.Fatal("want satisfied: failed is a terminal state")
		}
	})

	t.Run("all_completed blocks running", func(t *testing.T) {
		logs := map[string]*model.TaskLog{"u": log(model.TaskLogStatusRunning, recent)}
		ok, _ := DepsSatisfied([]string{"u"}, model.DepPolicyAllCompleted, base, lookup(logs))
		if ok {
			t.Fatal("want blocked: running is not terminal")
		}
	})

	t.Run("missing upstream blocks", func(t *testing.T) {
		ok, _ := DepsSatisfied([]string{"u"}, model.DepPolicyAllSuccess, base, lookup(nil))
		if ok {
			t.Fatal("want blocked: no upstream execution")
		}
	})

	t.Run("multiple deps one unmet", func(t *testing.T) {
		logs := map[string]*model.TaskLog{
			"u": log(model.TaskLogStatusSuccess, recent),
		}
		ok, reason := DepsSatisfied([]string{"u", "v"}, model.DepPolicyAllSuccess, base, lookup(logs))
		if ok {
			t.Fatal("want blocked: v missing")
		}
		if !strings.Contains(reason, "v") {
			t.Fatalf("reason should name missing upstream v: %s", reason)
		}
	})

	t.Run("no deps trivially satisfied", func(t *testing.T) {
		ok, _ := DepsSatisfied(nil, model.DepPolicyAllSuccess, base, lookup(nil))
		if !ok {
			t.Fatal("empty deps must be satisfied")
		}
	})
}
