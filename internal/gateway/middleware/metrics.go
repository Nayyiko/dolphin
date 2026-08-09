package middleware

import (
	"time"

	"github.com/yourname/dolphin/internal/gateway/router"
	"github.com/yourname/dolphin/internal/pkg/metrics"
)

// Metrics Prometheus 指标采集中间件。
// 记录请求 QPS、延迟、错误率（黄金信号的流量/延迟/错误）。
func Metrics() router.Middleware {
	return func(next router.HandlerFunc) router.HandlerFunc {
		return func(c *router.Context) {
			start := time.Now()

			// 饱和度：处理中请求数
			metrics.GatewayRequestsInFlight.Inc()
			defer metrics.GatewayRequestsInFlight.Dec()

			next(c)

			// 延迟 + 流量 + 错误
			duration := time.Since(start)
			metrics.RecordAPIMetrics(
				c.Request.Method,
				c.Request.URL.Path,
				c.Status,
				duration,
			)
		}
	}
}
