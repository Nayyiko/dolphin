// gateway 是 Dolphin 的 API 网关入口。
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/yourname/dolphin/internal/gateway/middleware"
	"github.com/yourname/dolphin/internal/gateway/proxy"
	"github.com/yourname/dolphin/internal/gateway/router"
	"github.com/yourname/dolphin/internal/pkg/config"
	"github.com/yourname/dolphin/internal/pkg/redisutil"
)

var (
	// 构建时注入的版本信息。
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

func main() {
	slog.Info("dolphin gateway starting",
		"version", Version,
		"build_time", BuildTime,
		"git_commit", GitCommit,
	)

	var configPath string
	flag.StringVar(&configPath, "config", "configs/gateway.yaml", "config file path")
	flag.Parse()

	var cfg config.GatewayConfig
	if err := config.Load(configPath, &cfg); err != nil {
		slog.Error("load config failed", "err", err)
		os.Exit(1)
	}

	r := router.NewRouter()

	// 中间件：Recovery 最外层（兜底 panic），Metrics 记录指标，Logger 记录访问。
	r.Use(middleware.Recovery(), middleware.Metrics(), middleware.Logger())

	// Redis 连接（限流用）。启动失败不致命——限流器 fail-open。
	rdb := redisutil.NewClient(cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB)
	if err := redisutil.Check(context.Background(), rdb); err != nil {
		slog.Warn("redis not available, rate limiter will fail-open", "err", err)
	}

	rl := middleware.NewRateLimiter(rdb, cfg.RateLimit.DefaultRate, cfg.RateLimit.DefaultCap, cfg.RateLimit.WindowSeconds, cfg.RateLimit.Enabled)
	r.Use(middleware.RateLimit(rl, "client_id"))

	// 反向代理：将 / 转发到 scheduler gRPC 转 HTTP 的地址（简化：直接转发到某个上游）。
	// 这里默认转发到 scheduler 的 http_port（metrics/health），实际管理 API 走 gRPC。
	// 为演示路由和代理能力，注册一个虚拟上游示例。
	upstreams := []string{"localhost:9090"}
	if cfg.Scheduler.Addr != "" {
		upstreams = []string{cfg.Scheduler.Addr}
	}
	rp := proxy.NewReverseProxy(upstreams, "rr")

	// 示例业务路由（真实项目会注册任务管理 API）。
	r.GET("/health", func(c *router.Context) {
		c.JSON(http.StatusOK, map[string]any{
			"status":    "ok",
			"version":   Version,
			"gitCommit": GitCommit,
		})
	})
	r.GET("/metrics", func(c *router.Context) {
		promhttp.Handler().ServeHTTP(c.Writer, c.Request)
	})
	r.GET("/", func(c *router.Context) {
		c.String(http.StatusOK, "dolphin gateway\n")
	})
	r.GET("/proxy/*rest", func(c *router.Context) {
		rp.ServeHTTP(c.Writer, c.Request)
	})

	httpServer := &http.Server{
		Addr:         ":" + fmt.Sprintf("%d", cfg.Server.Port),
		Handler:      r,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  cfg.Server.IdleTimeout,
	}

	go func() {
		slog.Info("gateway listening", "port", cfg.Server.Port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server error", "err", err)
			os.Exit(1)
		}
	}()

	// 优雅关闭
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	slog.Info("shutting down gateway...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		slog.Error("forced shutdown", "err", err)
	}
	slog.Info("gateway stopped")
}
