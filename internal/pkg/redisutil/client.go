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
		PoolSize:     20,
	})
}

// Check 验证连接是否可用。
func Check(ctx context.Context, cli *redis.Client) error {
	return cli.Ping(ctx).Err()
}
