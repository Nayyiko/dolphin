// worker 是 Dolphin 的任务执行器入口。
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

	"github.com/yourname/dolphin/internal/pkg/config"
	"github.com/yourname/dolphin/internal/pkg/etcdutil"
	"github.com/yourname/dolphin/internal/scheduler/election"
	"github.com/yourname/dolphin/internal/worker/client"
	"github.com/yourname/dolphin/internal/worker/discovery"
	"github.com/yourname/dolphin/internal/worker/executor"
)

var (
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

func main() {
	slog.Info("dolphin worker starting",
		"version", Version,
		"build_time", BuildTime,
		"git_commit", GitCommit,
	)

	var configPath string
	flag.StringVar(&configPath, "config", "configs/worker.yaml", "config file path")
	flag.Parse()

	var cfg config.WorkerConfig
	if err := config.Load(configPath, &cfg); err != nil {
		slog.Error("load config failed", "err", err)
		os.Exit(1)
	}

	// 生成 worker ID（模拟：用 hostname + 端口）
	hostname, _ := os.Hostname()
	workerID := fmt.Sprintf("%s-%d", hostname, os.Getpid())

	// 创建协程池
	pool := executor.NewPool(cfg.Pool.Capacity, nil)

	// 创建 gRPC 客户端
	clientCfg := client.Config{
		SchedulerAddr:  cfg.Scheduler.Addr,
		WorkerID:       workerID,
		Address:        "localhost:" + fmt.Sprintf("%d", cfg.Server.MetricsPort),
		MaxConcurrency: cfg.Pool.Capacity,
	}
	cl, err := client.New(clientCfg, pool)
	if err != nil {
		slog.Error("connect to scheduler failed", "err", err)
		os.Exit(1)
	}

	// 将结果上报器注入协程池
	pool.SetReporter(cl)

	// 运行客户端（注册 + 心跳 + 收任务）——阻塞直到 ctx 取消
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ── Leader 自动发现 ──
	// Worker 启动时可能早于 Scheduler Leader 就绪；etcd 是系统核心依赖，若不可用
	// 则回退到静态配置地址（保持向后兼容），不阻塞 Worker 启动。
	if len(cfg.Etcd.Endpoints) > 0 {
		etcdCli, err := etcdutil.NewClient(cfg.Etcd.Endpoints, cfg.Etcd.DialTimeout)
		if err != nil {
			slog.Warn("worker etcd unavailable, using static scheduler addr",
				"addr", cfg.Scheduler.Addr, "err", err)
		} else {
			defer etcdCli.Close()
			disc := discovery.New(etcdCli, election.LeaderAddrKey, func(_ context.Context, addr string) {
				cl.UpdateAddr(addr)
			})
			// 连接持续失败时，主动重新读取 leader 地址（兜底 watch 延迟/遗漏）。
			cl.Resolver = disc.ResolveNow
			go func() {
				if err := disc.Run(ctx); err != nil && ctx.Err() == nil {
					slog.Warn("leader discovery stopped", "err", err)
				}
			}()
			slog.Info("leader discovery enabled", "etcd_key", election.LeaderAddrKey)
		}
	}

	// metrics HTTP server
	httpServer := &http.Server{
		Addr:    ":" + fmt.Sprintf("%d", cfg.Server.MetricsPort),
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/healthz":
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("ok"))
			case "/metrics":
				promhttp.Handler().ServeHTTP(w, r)
			default:
				http.NotFound(w, r)
			}
		}),
	}
	go func() {
		slog.Info("worker metrics listening", "port", cfg.Server.MetricsPort)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server error", "err", err)
		}
	}()

	// 运行客户端
	go func() {
		if err := cl.Run(ctx); err != nil {
			slog.Error("worker client stopped", "err", err)
			cancel()
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	slog.Info("worker shutting down...")

	// 优雅关闭
	cancel()
	_ = cl.Close()
	pool.Shutdown()
	shutdownCtx, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	_ = httpServer.Shutdown(shutdownCtx)
	slog.Info("worker stopped")
}
