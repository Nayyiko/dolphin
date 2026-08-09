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

	"github.com/yourname/dolphin/internal/pkg/config"
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

	// HTTP Server
	httpServer := &http.Server{
		Addr:         ":" + itoa(cfg.Server.Port),
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

func itoa(i int) string {
	return fmt.Sprintf("%d", i)
}
