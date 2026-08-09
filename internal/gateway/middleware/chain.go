package middleware

import (
	"github.com/yourname/dolphin/internal/gateway/router"
)

// Chain 将多个中间件以洋葱模型包装 handler。
// Chain(handler, m1, m2) → m1(m2(handler))。
func Chain(handler router.HandlerFunc, middlewares ...router.Middleware) router.HandlerFunc {
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}
	return handler
}
