package proxy

import (
	"sync"
	"sync/atomic"
)

// LoadBalancer 负载均衡器。支持 Round Robin 和 Weighted Round Robin。
type LoadBalancer struct {
	algo     string
	targets  []string
	weights  []int
	rr       atomic.Uint64 // round-robin 游标
	mu       sync.RWMutex
}

// NewLoadBalancer 创建负载均衡器。
// algo: "rr" (Round Robin) 或 "wrr" (Weighted Round Robin)。
func NewLoadBalancer(targets []string, weights []int, algo string) *LoadBalancer {
	if len(weights) == 0 {
		weights = make([]int, len(targets))
		for i := range weights {
			weights[i] = 1
		}
	}
	return &LoadBalancer{
		algo:    algo,
		targets: targets,
		weights: weights,
	}
}

// Next 返回下一个目标地址。
func (lb *LoadBalancer) Next() string {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	n := len(lb.targets)
	if n == 0 {
		return ""
	}
	if n == 1 {
		return lb.targets[0]
	}

	switch lb.algo {
	case "wrr":
		return lb.nextWeighted(n)
	default: // "rr"
		idx := lb.rr.Add(1)
		return lb.targets[(idx-1)%uint64(n)]
	}
}

// nextWeighted 加权轮询。按权重展开为"权重总和"的循环空间。
func (lb *LoadBalancer) nextWeighted(n int) string {
	total := 0
	for _, w := range lb.weights {
		total += w
	}
	if total == 0 {
		return lb.targets[0]
	}

	idx := lb.rr.Add(1)
	pos := int((idx - 1) % uint64(total))
	acc := 0
	for i, w := range lb.weights {
		acc += w
		if pos < acc {
			return lb.targets[i]
		}
	}
	return lb.targets[0]
}

// UpdateTargets 动态更新目标列表（无锁热更新，供配置中心回调）。
func (lb *LoadBalancer) UpdateTargets(targets []string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.targets = targets
	lb.weights = make([]int, len(targets))
	for i := range lb.weights {
		lb.weights[i] = 1
	}
}
