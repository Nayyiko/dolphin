package proxy

import (
	"testing"
)

func TestLoadBalancer_RoundRobin(t *testing.T) {
	lb := NewLoadBalancer([]string{"a", "b", "c"}, nil, "rr")

	got := map[string]int{}
	for i := 0; i < 6; i++ {
		got[lb.Next()]++
	}
	// 每个目标应被选中 2 次
	for _, addr := range []string{"a", "b", "c"} {
		if got[addr] != 2 {
			t.Fatalf("addr %s selected %d times, want 2", addr, got[addr])
		}
	}
}

func TestLoadBalancer_Weighted(t *testing.T) {
	lb := NewLoadBalancer([]string{"a", "b"}, []int{3, 1}, "wrr")

	counts := map[string]int{}
	for i := 0; i < 400; i++ {
		counts[lb.Next()]++
	}
	// 权重 3:1，a 应约为 b 的 3 倍
	if counts["a"] < counts["b"]*2 {
		t.Fatalf("weighted distribution wrong: a=%d b=%d", counts["a"], counts["b"])
	}
}

func TestLoadBalancer_Empty(t *testing.T) {
	lb := NewLoadBalancer(nil, nil, "rr")
	if lb.Next() != "" {
		t.Fatalf("expected empty target")
	}
}
