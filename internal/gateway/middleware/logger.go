package middleware

import (
	"log/slog"
	"time"

	"github.com/yourname/dolphin/internal/gateway/router"
	"github.com/yourname/dolphin/internal/pkg/tracing"
)

// Logger 请求日志中间件。结构化日志，包含 trace_id 便于链路追踪。
func Logger() router.Middleware {
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
