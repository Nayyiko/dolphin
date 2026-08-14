package middleware

import (
	"log/slog"
	"sync"
	"time"

	"github.com/yourname/dolphin/internal/gateway/router"
	"github.com/yourname/dolphin/internal/pkg/tracing"
)

// Logger 请求日志中间件。结构化日志，包含 trace_id 便于链路追踪。
//
// 高 QPS 下每请求同步写一条日志会成为吞吐瓶颈：Go 的 net/http 在
// handler 链返回后才 flush 响应，慢速 Windows 控制台渲染日志会让
// handler 阻塞，导致响应延迟甚至超时（压测时表现为连接错误）。
// 因此对请求日志做限频采样：至少间隔 minLogInterval 才打一条，
// 突发流量下自动降频，避免刷屏拖垮网关。
func Logger() router.Middleware {
	const minLogInterval = 100 * time.Millisecond

	var (
		mu          sync.Mutex
		lastLogTime time.Time
	)

	return func(next router.HandlerFunc) router.HandlerFunc {
		return func(c *router.Context) {
			start := time.Now()

			// 从请求头读取或生成 trace_id
			traceID := c.Request.Header.Get(tracing.TraceIDHeader)
			if traceID == "" {
				traceID = tracing.NewTraceID()
			}
			c.Set("trace_id", traceID)

			next(c)

			// 限频采样：突发时最多 ~10 条/秒，避免同步写控制台阻塞请求
			mu.Lock()
			shouldLog := time.Since(lastLogTime) >= minLogInterval
			if shouldLog {
				lastLogTime = time.Now()
			}
			mu.Unlock()

			if shouldLog {
				slog.Info("http request",
					"trace_id", traceID,
					"method", c.Request.Method,
					"path", c.Request.URL.Path,
					"status", c.Status,
					"duration_ms", time.Since(start).Milliseconds(),
					"remote", c.Request.RemoteAddr,
				)
			}
		}
	}
}
