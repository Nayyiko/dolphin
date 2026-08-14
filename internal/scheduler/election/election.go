package election

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/yourname/dolphin/internal/pkg/metrics"
)

// LeaderAddrKey 保存当前 Leader gRPC 地址的 etcd key。
// Worker 监听此 key 实现 Leader 自动发现与切换。
const LeaderAddrKey = "/dolphin/scheduler/leader-addr"

// LeaderElector 基于 etcd Lease + Campaign 的 Leader 选举器。
//
// 原理:
//   - 每个实例创建带 TTL 的 Session（等价于持有 Lease）。
//   - Campaign 阻塞直到该实例成为 Leader。
//   - 后台 goroutine 持续续约（每 TTL/3）。
//   - 若网络分区导致续约失败，Lease 过期 → 其他实例可竞选成功。
//   - 旧 Leader 的写入会因 Lease 过期被 etcd 拒绝 → 天然防脑裂。
type LeaderElector struct {
	client    *clientv3.Client
	prefix    string
	candidate string
	ttl       int

	session  *concurrency.Session
	election *concurrency.Election

	// mu 保护 session/election 的并发访问。
	// Publish 可能在新一轮 Campaign 替换 session 的同时读取旧 session，需要加锁。
	mu sync.Mutex

	isLeader atomic.Bool

	onBecomeLeader func(ctx context.Context)
	onLoseLeader   func()
}

// NewLeaderElector 创建选举器。
func NewLeaderElector(client *clientv3.Client, prefix, candidate string, ttl int) *LeaderElector {
	return &LeaderElector{
		client:    client,
		prefix:    prefix,
		candidate: candidate,
		ttl:       ttl,
	}
}

// SetOnBecomeLeader 当选回调。
func (le *LeaderElector) SetOnBecomeLeader(fn func(ctx context.Context)) {
	le.onBecomeLeader = fn
}

// SetOnLoseLeader 退位回调。
func (le *LeaderElector) SetOnLoseLeader(fn func()) {
	le.onLoseLeader = fn
}

// IsLeader 当前节点是否为 Leader。
func (le *LeaderElector) IsLeader() bool {
	return le.isLeader.Load()
}

// Publish 将 Leader 的 gRPC 服务地址写入 etcd，并绑定到选主 session 的 lease。
// Leader 宕机 / 失去租约时，key 随 lease 自动过期删除；
// Worker 通过监听该 key 发现并自动切换到新 Leader（故障转移的关键一环）。
func (le *LeaderElector) Publish(ctx context.Context, key, value string) error {
	le.mu.Lock()
	session := le.session
	le.mu.Unlock()
	if session == nil {
		return fmt.Errorf("no session yet")
	}
	_, err := le.client.Put(ctx, key, value, clientv3.WithLease(session.Lease()))
	return err
}

// LeaseID 返回当前选主 session 的 lease ID（未就绪时返回 0）。
func (le *LeaderElector) LeaseID() clientv3.LeaseID {
	le.mu.Lock()
	defer le.mu.Unlock()
	if le.session == nil {
		return clientv3.NoLease
	}
	return le.session.Lease()
}

// Campaign 参与选举。阻塞直到成为 Leader 或 ctx 取消。
func (le *LeaderElector) Campaign(ctx context.Context) error {
	session, err := concurrency.NewSession(le.client,
		concurrency.WithTTL(le.ttl),
		concurrency.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}

	election := concurrency.NewElection(session, le.prefix)

	if err := election.Campaign(ctx, le.candidate); err != nil {
		if err == context.Canceled || ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("campaign: %w", err)
	}

	le.mu.Lock()
	le.session = session
	le.election = election
	le.mu.Unlock()

	// 当选 Leader
	le.isLeader.Store(true)
	metrics.SchedulerLeaderElections.Inc()
	slog.Info("became leader",
		"candidate", le.candidate,
		"election_key", election.Key(),
		"lease_id", session.Lease())

	// 监听 session 断开
	go le.watchSession(ctx)

	if le.onBecomeLeader != nil {
		le.onBecomeLeader(ctx)
	}
	return nil
}

// watchSession 监听 session 断开（Lease 过期/连接断开）。
// 断开后清理 Leader 状态，并自动重新竞选。
func (le *LeaderElector) watchSession(ctx context.Context) {
	le.mu.Lock()
	session := le.session
	le.mu.Unlock()
	if session == nil {
		return
	}

	select {
	case <-session.Done():
		le.isLeader.Store(false)
		slog.Warn("leader session lost, re-campaigning", "candidate", le.candidate)
		if le.onLoseLeader != nil {
			le.onLoseLeader()
		}
		// 短暂延迟后重新竞选
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			return
		}
		_ = session.Close()
		_ = le.Campaign(ctx)
	case <-ctx.Done():
		le.Resign()
	}
}

// Resign 主动让位。
func (le *LeaderElector) Resign() {
	le.mu.Lock()
	election := le.election
	session := le.session
	le.election = nil
	le.session = nil
	le.mu.Unlock()

	if election != nil {
		_ = election.Resign(context.Background())
	}
	if session != nil {
		_ = session.Close()
	}
	le.isLeader.Store(false)
}
