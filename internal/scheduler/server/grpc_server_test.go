package server

import (
	"testing"
	"time"
)

// TestOnlineWorkersUsesInMemoryRegistry 验证 OnlineWorkers 只返回本实例内存注册表
// 中持有活跃流的 worker（不再从 MySQL 读），并携带实时负载。
func TestOnlineWorkersUsesInMemoryRegistry(t *testing.T) {
	s := NewSchedulerService(nil) // OnlineWorkers 不访问 manager/DB

	s.mu.Lock()
	s.workers["w1"] = &workerConn{id: "w1", lastSeen: time.Now(), currentLoad: 3, maxConcurrency: 10}
	s.workers["w2"] = &workerConn{id: "w2", lastSeen: time.Now(), currentLoad: 0, maxConcurrency: 50}
	s.mu.Unlock()

	ws := s.OnlineWorkers()
	if len(ws) != 2 {
		t.Fatalf("expected 2 workers from in-memory registry, got %d", len(ws))
	}

	byID := map[string]*struct{ load, maxConc int }{}
	for _, w := range ws {
		if w.Status != "online" {
			t.Errorf("worker %s status = %q, want online", w.ID, w.Status)
		}
		byID[w.ID] = &struct{ load, maxConc int }{w.CurrentLoad, w.MaxConcurrency}
	}

	if byID["w1"] == nil || byID["w1"].load != 3 || byID["w1"].maxConc != 10 {
		t.Fatalf("w1 not reflected correctly: %+v", byID["w1"])
	}
	if byID["w2"] == nil || byID["w2"].load != 0 || byID["w2"].maxConc != 50 {
		t.Fatalf("w2 not reflected correctly: %+v", byID["w2"])
	}

	// 内存注册表为空 → 返回空列表（不误报 DB 里的历史 online worker）
	s.mu.Lock()
	s.workers = make(map[string]*workerConn)
	s.mu.Unlock()
	if ws := s.OnlineWorkers(); len(ws) != 0 {
		t.Fatalf("expected empty registry to return no workers, got %d", len(ws))
	}
}

// TestDispatchToUnregisteredWorkerFails 未连接的 worker 不应被 Dispatch 命中
// （Dispatch 依赖内存 map，与 OnlineWorkers 同源）。
func TestDispatchToUnregisteredWorkerFails(t *testing.T) {
	s := NewSchedulerService(nil)
	if err := s.Dispatch("ghost", nil); err == nil {
		t.Fatal("expected dispatch to unregistered worker to fail")
	}
}
