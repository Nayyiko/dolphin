package redisutil

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// NewClient 创建 Redis 客户端。
func NewClient(addr, password string, db int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		// 连接池：默认 20 在压测并发下会排队阻塞。
		// 网关压测并发 50，池子需要容纳这些并发连接。
		PoolSize:     100,
		// 池满时等待获取连接的最长时间，避免无限阻塞拖垮请求。
		PoolTimeout:  500 * time.Millisecond,
		MinIdleConns: 10,
	})
}

// Check 验证连接是否可用。
func Check(ctx context.Context, cli *redis.Client) error {
	return cli.Ping(ctx).Err()
}
