// Package discovery 实现 Worker 对 Scheduler Leader 地址的自动发现。
//
// 架构背景:
//   集群中有多个 Scheduler，任意时刻只有一个 Leader（通过 etcd 选主）。
//   若 Worker 在配置里硬编码 Leader 的 gRPC 地址，Leader 宕机后新 Leader
//   地址不同（或端口不同），Worker 将无法重新连接——这是故障转移的架构缺口。
//
// 方案:
//   - Leader 当选后，将自身 gRPC 地址写入 etcd key（带选主 session 的 lease，
//     随 Leader 死亡自动过期）。
//   - Worker 监听该 key，地址变化时自动切换到新 Leader。
//
// 效果: Worker 无需感知集群中有几个 Scheduler、谁在何时成为 Leader，
//   与 K8s 中 kubelet 通过 API Server 发现 apiserver 的机制同构。
package discovery

import (
	"context"
	"errors"
	"log/slog"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Discoverer 监听 etcd 中的 Leader 地址 key，并在变化时回调。
type Discoverer struct {
	cli      *clientv3.Client
	key      string
	onUpdate func(ctx context.Context, addr string)
}

// New 创建 Discoverer。
// key 为 etcd 中保存 Leader 地址的 key，onUpdate 在地址变化时被调用。
func New(cli *clientv3.Client, key string, onUpdate func(ctx context.Context, addr string)) *Discoverer {
	return &Discoverer{cli: cli, key: key, onUpdate: onUpdate}
}

// Run 启动监听循环。先读取当前值（bootstrap），再持续 watch 变更。
// 仅在 ctx 取消时返回。
func (d *Discoverer) Run(ctx context.Context) error {
	if d.cli == nil {
		return errors.New("nil etcd client")
	}

	// 1. 启动时先 Get 一次，拿到当前值及 revision。
	//    随后从 rev+1 开始 watch，避免 bootstrap 与 watch 之间的变更被漏掉。
	rev := int64(0)
	resp, err := d.cli.Get(ctx, d.key)
	if err != nil {
		slog.Warn("leader discovery bootstrap failed, will rely on watch", "err", err)
	} else {
		rev = resp.Header.Revision
		for _, kv := range resp.Kvs {
			if kv.Value != nil {
				slog.Info("leader addr bootstrapped", "addr", string(kv.Value), "rev", kv.ModRevision)
				d.onUpdate(ctx, string(kv.Value))
			}
		}
	}

	// 2. 持续 watch。rev+1 保证不漏掉 bootstrap 之后到 watch 建立之间的写入。
	wch := d.cli.Watch(ctx, d.key, clientv3.WithRev(rev+1))
	for resp := range wch {
		if err := resp.Err(); err != nil {
			slog.Warn("leader discovery watch error", "err", err)
			// 轻微延迟后重启 watch，避免 hot-loop
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
			wch = d.cli.Watch(ctx, d.key, clientv3.WithRev(0))
			continue
		}
		for _, ev := range resp.Events {
			if ev.Type == clientv3.EventTypePut {
				addr := string(ev.Kv.Value)
				slog.Info("leader addr discovered", "addr", addr, "rev", ev.Kv.ModRevision)
				d.onUpdate(ctx, addr)
			}
			// EventTypeDelete 忽略：保持当前地址，等待新 Leader 写入。
		}
	}
	return ctx.Err()
}
