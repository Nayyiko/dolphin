// scheduler 是 Dolphin 的调度引擎入口。
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/yourname/dolphin/internal/pkg/config"
	"github.com/yourname/dolphin/internal/pkg/etcdutil"
	"github.com/yourname/dolphin/internal/pkg/metrics"
	"github.com/yourname/dolphin/internal/pkg/model"
	"github.com/yourname/dolphin/internal/scheduler/cron"
	"github.com/yourname/dolphin/internal/scheduler/election"
	"github.com/yourname/dolphin/internal/scheduler/executor"
	"github.com/yourname/dolphin/internal/scheduler/informer"
	"github.com/yourname/dolphin/internal/scheduler/manager"
	"github.com/yourname/dolphin/internal/scheduler/queue"
	"github.com/yourname/dolphin/internal/scheduler/reconciler"
	"github.com/yourname/dolphin/internal/scheduler/server"

	"google.golang.org/grpc"
)

var (
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

func main() {
	slog.Info("dolphin scheduler starting",
		"version", Version,
		"build_time", BuildTime,
		"git_commit", GitCommit,
	)

	var configPath string
	flag.StringVar(&configPath, "config", "configs/scheduler.yaml", "config file path")
	flag.Parse()

	var cfg config.SchedulerConfig
	if err := config.Load(configPath, &cfg); err != nil {
		slog.Error("load config failed", "err", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ── MySQL ──
	db, err := gorm.Open(mysql.Open(cfg.MySQL.DSN), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Warn),
	})
	if err != nil {
		slog.Error("connect mysql failed", "err", err)
		os.Exit(1)
	}

	// ── etcd ──
	etcdCli, err := etcdutil.NewClient(cfg.Etcd.Endpoints, cfg.Etcd.DialTimeout)
	if err != nil {
		slog.Error("connect etcd failed", "err", err)
		os.Exit(1)
	}
	defer etcdCli.Close()

	// ── Manager + 建表 ──
	mgr := manager.NewManager(db)
	if err := mgr.AutoMigrate(); err != nil {
		slog.Error("auto migrate failed", "err", err)
		os.Exit(1)
	}

	// ── gRPC Service ──
	svc := server.NewSchedulerService(mgr)
	svc.SetCronNext(func(expr string) (time.Time, error) {
		sched, err := cron.Parse(expr)
		if err != nil {
			return time.Time{}, err
		}
		return sched.Next(time.Now()), nil
	})

	// ── Informer（List + Watch → 本地缓存）──
	lister := func(ctx context.Context) ([]model.Task, error) {
		var tasks []model.Task
		if err := db.WithContext(ctx).Where("deletion_timestamp IS NULL").Find(&tasks).Error; err != nil {
			return nil, err
		}
		return tasks, nil
	}
	watcher := func(ctx context.Context, since time.Time) ([]model.Task, error) {
		var tasks []model.Task
		if err := db.WithContext(ctx).
			Where("updated_at > ? AND deletion_timestamp IS NULL", since).
			Find(&tasks).Error; err != nil {
			return nil, err
		}
		return tasks, nil
	}
	inf := informer.NewTaskInformer(lister, watcher, time.Second)
	if err := inf.Start(ctx); err != nil {
		slog.Error("informer start failed", "err", err)
		os.Exit(1)
	}

	// ── WorkQueue ──
	q := queue.NewWorkQueue(5*time.Second, 5*time.Minute)

	// ── Executor（选择 Worker 并下发）──
	exec := executor.NewTaskExecutor(svc, executor.LeastLoadedSelector{})

	// ── Reconciler ──
	recon := reconciler.NewReconciler(q, inf.GetLister(), mgr, exec,
		func(expr string) (time.Time, error) {
			sched, err := cron.Parse(expr)
			if err != nil {
				return time.Time{}, err
			}
			return sched.Next(time.Now()), nil
		},
		cfg.Reconciler.Workers,
	)

	// Informer 事件 → 入队
	inf.AddEventHandler(eventHandler{recon: recon})

	// 启动 Reconciler（仅在当前节点是 Leader 时运行核心调度）
	// 简化：非 Leader 节点只运行 informer 同步，不跑 reconciler 主循环。
	leaderElector := election.NewLeaderElector(etcdCli, "/dolphin/scheduler/leader",
		hostname(), cfg.Election.TTL)
	leaderElector.SetOnBecomeLeader(func(ctx context.Context) {
		slog.Info("starting reconciler as leader")
		go recon.Run(ctx)
	})
	leaderElector.SetOnLoseLeader(func() {
		slog.Warn("lost leadership")
	})
	go func() {
		if err := leaderElector.Campaign(ctx); err != nil && err != context.Canceled {
			slog.Error("leader election failed", "err", err)
		}
	}()

	// 周期更新 leader 指标
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if leaderElector.IsLeader() {
					metrics.SchedulerIsLeader.Set(1)
				} else {
					metrics.SchedulerIsLeader.Set(0)
				}
			}
		}
	}()

	// ── gRPC Server ──
	grpcServer := grpc.NewServer()
	svc.RegisterServer(grpcServer)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Server.GRPCPort))
	if err != nil {
		slog.Error("listen grpc failed", "err", err)
		os.Exit(1)
	}
	go func() {
		slog.Info("scheduler gRPC listening", "port", cfg.Server.GRPCPort)
		if err := grpcServer.Serve(lis); err != nil {
			slog.Error("grpc serve error", "err", err)
		}
	}()

	// ── metrics/health HTTP Server ──
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Server.HTTPPort),
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
		slog.Info("scheduler metrics listening", "port", cfg.Server.HTTPPort)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server error", "err", err)
		}
	}()

	// ── 优雅关闭 ──
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	slog.Info("scheduler shutting down...")

	cancel() // 取消所有后台 goroutine
	leaderElector.Resign()
	grpcServer.GracefulStop()
	shutdownCtx, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	_ = httpServer.Shutdown(shutdownCtx)
	slog.Info("scheduler stopped")
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return fmt.Sprintf("scheduler-%d", os.Getpid())
	}
	return fmt.Sprintf("%s-%d", h, os.Getpid())
}

// eventHandler 将 Informer 事件转发给 Reconciler。
type eventHandler struct {
	recon *reconciler.Reconciler
}

func (h eventHandler) OnAdd(item *informer.TaskItem) {
	h.recon.EnqueueTask(item.Task.ID)
}
func (h eventHandler) OnUpdate(_, newItem *informer.TaskItem) {
	h.recon.EnqueueTask(newItem.Task.ID)
}
func (h eventHandler) OnDelete(item *informer.TaskItem) {
	h.recon.EnqueueTask(item.Task.ID)
}
