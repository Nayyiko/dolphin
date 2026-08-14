package client

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestMaybeResolveThreshold 验证 Resolver 只在 backoff 增长到阈值后才被调用，
// 避免正常抖动时对 etcd 造成无谓压力。
func TestMaybeResolveThreshold(t *testing.T) {
	var calls int32
	c := &Client{Resolver: func() { atomic.AddInt32(&calls, 1) }}

	c.maybeResolve(1 * time.Second)
	c.maybeResolve(2 * time.Second)
	c.maybeResolve(4 * time.Second)
	if n := atomic.LoadInt32(&calls); n != 0 {
		t.Fatalf("expected no resolve below threshold, got %d", n)
	}

	c.maybeResolve(8 * time.Second)
	c.maybeResolve(16 * time.Second)
	c.maybeResolve(30 * time.Second)
	if n := atomic.LoadInt32(&calls); n != 3 {
		t.Fatalf("expected 3 resolves at/above threshold, got %d", n)
	}
}

// TestMaybeResolveNilResolver nil Resolver 不应 panic。
func TestMaybeResolveNilResolver(t *testing.T) {
	c := &Client{}
	c.maybeResolve(30 * time.Second)
}
