package etcdutil

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// NewClient 创建 etcd 客户端。
func NewClient(endpoints []string, dialTimeout time.Duration) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
	})
}

// PutWithLease 写入一个带 TTL 的 key。
func PutWithLease(ctx context.Context, cli *clientv3.Client, key, value string, ttl int64) (clientv3.LeaseID, error) {
	lease, err := cli.Grant(ctx, ttl)
	if err != nil {
		return 0, err
	}
	_, err = cli.Put(ctx, key, value, clientv3.WithLease(lease.ID))
	if err != nil {
		return 0, err
	}
	return lease.ID, nil
}

// CompareAndSwap 基于 mod revision 的乐观锁写入。
// 仅当 key 的当前 mod revision == expectRevision 时才写入。
// 返回是否成功。用于防止并发覆盖（如 Leader 写入冲突检测）。
func CompareAndSwap(ctx context.Context, cli *clientv3.Client, key, value string, expectRevision int64) (bool, error) {
	txn := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", expectRevision)).
		Then(clientv3.OpPut(key, value))
	resp, err := txn.Commit()
	if err != nil {
		return false, err
	}
	return resp.Succeeded, nil
}

// WatchPrefix 监听某个前缀下的所有变更，返回一个只读的变更通道。
func WatchPrefix(ctx context.Context, cli *clientv3.Client, prefix string) clientv3.WatchChan {
	return cli.Watch(ctx, prefix, clientv3.WithPrefix())
}
