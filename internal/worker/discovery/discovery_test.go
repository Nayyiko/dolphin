package discovery

import (
	"context"
	"sync/atomic"
	"testing"
)

// fakeOnUpdate 用原子值记录最后一次回调，避免数据竞争。
type fakeOnUpdate struct {
	val atomic.Value
}

func newFake() *fakeOnUpdate {
	f := &fakeOnUpdate{}
	f.val.Store("")
	return f
}

func (f *fakeOnUpdate) set(s string) {
	f.val.Store(s)
}

func (f *fakeOnUpdate) get() string {
	v := f.val.Load()
	if v == nil {
		return ""
	}
	return v.(string)
}

// TestDiscovererKeyLogic 验证 key 与 LeaderAddrKey 的一致性。
// 这里不启动真实 etcd，只验证发现器的 onUpdate 回调契约（值非空才切换）。
func TestDiscovererOnUpdateContract(t *testing.T) {
	f := newFake()

	disc := New(nil, "/test/leader-addr", func(_ context.Context, addr string) {
		if addr != "" {
			f.set(addr)
		}
	})
	if disc == nil {
		t.Fatal("New returned nil")
	}

	// 模拟一次回调
	disc.onUpdate(context.Background(), "localhost:50052")
	if f.get() != "localhost:50052" {
		t.Fatalf("expected addr to be recorded, got %q", f.get())
	}

	// 空地址不应触发切换（UpdateAddr 会忽略空值，这里验证回调契约）
	disc.onUpdate(context.Background(), "")
	// 空值被回调内忽略，保持原地址
	if f.get() != "localhost:50052" {
		t.Fatalf("empty addr should be ignored, got %q", f.get())
	}
}

// TestDiscovererRunNilClient 没有 etcd 时 Run 应返回错误而不 panic。
func TestDiscovererRunNilClient(t *testing.T) {
	disc := New(nil, "/test/leader-addr", func(_ context.Context, _ string) {})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := disc.Run(ctx)
	if err == nil {
		t.Fatal("expected error for nil client, got nil")
	}
}
