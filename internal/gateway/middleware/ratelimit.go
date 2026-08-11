package middleware

import (
	"context"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yourname/dolphin/internal/gateway/router"
	"github.com/yourname/dolphin/internal/pkg/metrics"
)

// tokenBucketLua 令牌桶限流脚本。
// 用 Lua 保证"读取-计算-写入"的原子性，避免多实例竞态。
// KEYS[1]: 桶 key
// ARGV[1]: rate (tokens/sec)
// ARGV[2]: capacity (burst)
// ARGV[3]: now (unix seconds)
// 返回 1=通过, 0=拒绝。
const tokenBucketLua = `
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local lastRefill = tonumber(redis.call('HGET', key, 'last_refill') or now)
local tokens = tonumber(redis.call('HGET', key, 'tokens') or capacity)

local elapsed = now - lastRefill
local refill = elapsed * rate
if refill > 0 then
    tokens = math.min(capacity, tokens + refill)
end

if tokens >= 1 then
    tokens = tokens - 1
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
    redis.call('EXPIRE', key, 60)
    return 1
else
    redis.call('HSET', key, 'last_refill', now)
    redis.call('EXPIRE', key, 60)
    return 0
end
`

// RateLimiter 令牌桶限流器。
type RateLimiter struct {
	rdb      *redis.Client
	rate     int   // tokens/sec
	capacity int   // burst
	window   int64 // key 过期窗口（秒）
	enabled  bool
}

// NewRateLimiter 创建限流器。
func NewRateLimiter(rdb *redis.Client, rate, capacity int, windowSeconds int64, enabled bool) *RateLimiter {
	return &RateLimiter{
		rdb:      rdb,
		rate:     rate,
		capacity: capacity,
		window:   windowSeconds,
		enabled:  enabled,
	}
}

// Allow 判断 key 是否允许通过。key 形如 "client_id:endpoint"。
func (rl *RateLimiter) Allow(ctx context.Context, key string) (bool, error) {
	if !rl.enabled {
		return true, nil
	}
	now := time.Now().Unix()
	res, err := rl.rdb.Eval(ctx, tokenBucketLua, []string{"ratelimit:" + key},
		rl.rate, rl.capacity, now).Int()
	if err != nil {
		// Redis 故障时放行（fail-open），避免限流器成为单点。
		return true, err
	}
	return res == 1, nil
}

// RateLimit 限流中间件。按 client_id + endpoint 二级限流。
// clientIDKey 指定从 Context 中读取 client_id 的 key。
func RateLimit(rl *RateLimiter, clientIDKey string) router.Middleware {
	return func(next router.HandlerFunc) router.HandlerFunc {
		return func(c *router.Context) {
			clientID, _ := c.Get(clientIDKey)
			cid, _ := clientID.(string)
			if cid == "" {
				cid = "anonymous"
			}
			key := cid + ":" + c.Request.URL.Path

			allowed, _ := rl.Allow(c.Request.Context(), key)
			if !allowed {
				// 记录限流拒绝指标（按 client + endpoint）
				metrics.GatewayRatelimitRejectedTotal.WithLabelValues(cid, c.Request.URL.Path).Inc()
				c.JSON(http.StatusTooManyRequests, map[string]any{
					"code":    "rate_limited",
					"message": "too many requests",
				})
				c.Abort()
				return
			}
			next(c)
		}
	}
}
