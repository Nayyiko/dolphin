package middleware

import (
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"

	"github.com/yourname/dolphin/internal/gateway/router"
)

// Recovery Panic 恢复中间件。防止单个 handler panic 拖垮整个进程。
func Recovery() router.Middleware {
	return func(next router.HandlerFunc) router.HandlerFunc {
		return func(c *router.Context) {
			defer func() {
				if r := recover(); r != nil {
					slog.Error("panic recovered",
						"panic", r,
						"path", c.Request.URL.Path,
						"stack", string(debug.Stack()),
					)
					c.Status = http.StatusInternalServerError
					c.JSON(http.StatusInternalServerError, map[string]any{
						"code":    "internal_error",
						"message": fmt.Sprintf("panic: %v", r),
					})
					c.Abort()
				}
			}()
			next(c)
		}
	}
}
