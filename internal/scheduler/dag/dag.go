// Package dag 提供 DAG 依赖编排的图算法与依赖门控判定。
//
// 语义说明（task-level dependency，非完整 workflow-run）：
//
//	任务通过 DependOn 声明上游。一个任务"依赖满足"当且仅当每个上游在
//	"本任务上次运行之后"有符合策略的执行。这个"新鲜度"条件保证依赖
//	不会串到上一次运行的旧结果（Makefile 式语义）。
//
// 三个入口：
//   - ValidateDependOn: 创建/更新任务时的图校验（环 + 悬空引用 + 策略）。
//   - Graph: 从全量任务构建依赖图，提供拓扑序与上下游查询。
//   - DepsSatisfied: 运行时依赖门控的纯函数（由 Reconciler 注入 DB 查询）。
package dag

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/yourname/dolphin/internal/pkg/model"
)

// ErrCycle 依赖图存在环。
var ErrCycle = errors.New("dag cycle detected")

// ErrInvalidDepPolicy 非法依赖策略。
var ErrInvalidDepPolicy = errors.New("invalid dep policy")

// ErrDependencyNotFound 依赖的上游任务不存在（悬空引用）。
var ErrDependencyNotFound = errors.New("dependency task not found")

// ParseDependOn 将数据库中的 JSON 数组字符串解析为上游任务 ID 列表。
// 空串 / 空数组返回 nil。
func ParseDependOn(s string) ([]string, error) {
	if s == "" {
		return nil, nil
	}
	var ids []string
	if err := json.Unmarshal([]byte(s), &ids); err != nil {
		return nil, fmt.Errorf("parse depend_on %q: %w", s, err)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return ids, nil
}

// MarshalDependOn 将上游任务 ID 列表序列化为 JSON 数组字符串。
func MarshalDependOn(ids []string) string {
	if len(ids) == 0 {
		return ""
	}
	b, _ := json.Marshal(ids)
	return string(b)
}

// Graph DAG 依赖图。节点 = 任务 ID，边 U→D 表示 D 依赖 U。
type Graph struct {
	// edges: upstream → 直接依赖它的下游任务列表。
	edges map[string][]string
	// rev: task → 它直接依赖的上游任务列表。
	rev map[string][]string
	// tasks: id → task（仅未删除任务）。
	tasks map[string]model.Task
}

// NewGraph 从任务列表构建依赖图。只纳入未删除的任务。
// 若任务携带非法 DepPolicy，或存在自依赖/空依赖，返回错误。
func NewGraph(tasks []model.Task) (*Graph, error) {
	g := &Graph{
		edges: make(map[string][]string),
		rev:   make(map[string][]string),
		tasks: make(map[string]model.Task),
	}
	for _, t := range tasks {
		if t.DeletionTimestamp != nil {
			continue
		}
		g.tasks[t.ID] = t
		deps, err := ParseDependOn(t.DependOn)
		if err != nil {
			return nil, fmt.Errorf("task %s: %w", t.ID, err)
		}
		if len(deps) == 0 {
			continue
		}
		if !model.DepPolicyValues[t.DepPolicy] {
			return nil, fmt.Errorf("task %s: %w %q", t.ID, ErrInvalidDepPolicy, t.DepPolicy)
		}
		for _, up := range deps {
			if up == "" || up == t.ID {
				return nil, fmt.Errorf("task %s: self/empty dependency %q", t.ID, up)
			}
			if !contains(g.rev[t.ID], up) {
				g.rev[t.ID] = append(g.rev[t.ID], up)
				g.edges[up] = append(g.edges[up], t.ID)
			}
		}
	}
	return g, nil
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

// HasCycle 检测图中是否存在环，返回 (环路径, 是否存在环)。
// 使用 DFS 沿依赖边回走，返回一个具体的环路径（如 [A, C, B, A]）。
func (g *Graph) HasCycle() ([]string, bool) {
	if len(g.tasks) == 0 {
		return nil, false
	}
	visiting := make(map[string]bool, len(g.tasks))
	done := make(map[string]bool, len(g.tasks))
	var stack []string

	var dfs func(id string) ([]string, bool)
	dfs = func(id string) ([]string, bool) {
		visiting[id] = true
		stack = append(stack, id)
		for _, dep := range g.rev[id] {
			if done[dep] {
				continue
			}
			if visiting[dep] {
				// 找到环：截取 stack 中 dep 到栈顶，再补回 dep 形成闭合路径。
				for i, x := range stack {
					if x == dep {
						path := append([]string{}, stack[i:]...)
						path = append(path, dep)
						return path, true
					}
				}
			}
			if p, ok := dfs(dep); ok {
				return p, true
			}
		}
		stack = stack[:len(stack)-1]
		visiting[id] = false
		done[id] = true
		return nil, false
	}

	for id := range g.tasks {
		if !done[id] {
			if p, ok := dfs(id); ok {
				return p, true
			}
		}
	}
	return nil, false
}

// TopoOrder 返回拓扑排序（依赖在前、被依赖在后）。存在环时返回 ErrCycle。
func (g *Graph) TopoOrder() ([]string, error) {
	if path, ok := g.HasCycle(); ok {
		return nil, fmt.Errorf("%w: %v", ErrCycle, path)
	}
	indeg := make(map[string]int, len(g.tasks))
	for id := range g.tasks {
		indeg[id] = len(g.rev[id])
	}
	queue := make([]string, 0)
	for id, d := range indeg {
		if d == 0 {
			queue = append(queue, id)
		}
	}
	order := make([]string, 0, len(g.tasks))
	for len(queue) > 0 {
		u := queue[0]
		queue = queue[1:]
		order = append(order, u)
		for _, v := range g.edges[u] {
			indeg[v]--
			if indeg[v] == 0 {
				queue = append(queue, v)
			}
		}
	}
	return order, nil
}

// DependentsOf 返回直接依赖 taskID 的下游任务列表。
func (g *Graph) DependentsOf(taskID string) []string {
	return g.edges[taskID]
}

// DependenciesOf 返回 taskID 直接依赖的上游任务列表。
func (g *Graph) DependenciesOf(taskID string) []string {
	return g.rev[taskID]
}

// ValidateDependOn 校验把 candidate 加入现有任务后的依赖图。
// 校验项：
//  1. candidate 依赖策略合法；
//  2. 依赖的上游任务存在（无悬空引用，含 candidate 自身）；
//  3. 全量图无环（返回环路径）。
func ValidateDependOn(all []model.Task, candidate *model.Task) error {
	deps, err := ParseDependOn(candidate.DependOn)
	if err != nil {
		return err
	}
	if len(deps) == 0 {
		return nil
	}
	if !model.DepPolicyValues[candidate.DepPolicy] {
		return fmt.Errorf("%w: %q", ErrInvalidDepPolicy, candidate.DepPolicy)
	}

	// 悬空引用检查：上游必须存在于现有任务或 candidate 本身。
	ids := make(map[string]bool, len(all)+1)
	for _, t := range all {
		ids[t.ID] = true
	}
	ids[candidate.ID] = true
	for _, up := range deps {
		if !ids[up] {
			return fmt.Errorf("%w: %s", ErrDependencyNotFound, up)
		}
	}

	tasks := make([]model.Task, 0, len(all)+1)
	tasks = append(tasks, all...)
	tasks = append(tasks, *candidate)
	g, err := NewGraph(tasks)
	if err != nil {
		return err
	}
	if path, ok := g.HasCycle(); ok {
		return fmt.Errorf("%w: %v", ErrCycle, path)
	}
	return nil
}

// LogLookup 查询上游 taskID 在 after 之后最近一次执行；不存在返回 nil。
type LogLookup func(taskID string, after time.Time) *model.TaskLog

// DepsSatisfied 运行时依赖门控的纯函数。
//   - deps: 上游任务 ID 列表
//   - policy: all_success / all_completed
//   - lastRun: 本任务上次运行的开始时间（从未运行则为零值）
//   - lookup: 返回上游在 lastRun 之后最近一次执行
//
// 返回 (是否满足, 未满足原因)。任何上游缺失最近执行或状态不满足策略 → 不满足。
func DepsSatisfied(deps []string, policy string, lastRun time.Time, lookup LogLookup) (bool, string) {
	for _, up := range deps {
		l := lookup(up, lastRun)
		switch policy {
		case model.DepPolicyAllCompleted:
			if l == nil || !terminalStatus(l.Status) {
				return false, fmt.Sprintf("upstream %s has no completed execution after my last run", up)
			}
		default: // all_success
			if l == nil || l.Status != model.TaskLogStatusSuccess {
				return false, fmt.Sprintf("upstream %s has no successful execution after my last run", up)
			}
		}
	}
	return true, ""
}

// terminalStatus 判断状态是否为终止态（成功/失败/超时）。
func terminalStatus(s string) bool {
	switch s {
	case model.TaskLogStatusSuccess, model.TaskLogStatusFailed, model.TaskLogStatusTimeout:
		return true
	}
	return false
}
