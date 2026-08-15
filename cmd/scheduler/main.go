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
	"strconv"
	"sync"
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

// failCounts 记录 /debug/fail 每个路径已合成的失败次数（并发安全），
// 用于测试"前 N 次失败、之后成功"的执行级重试路径。
var failCounts sync.Map // map[string]int

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

	// DAG 事件驱动 + 执行级重试：任务结果到达 → 推送下游 + 失败自动重试。
	svc.SetOnTaskResult(func(taskID, status string) {
		recon.OnTaskResult(taskID, status)
	})

	// 启动 Reconciler（仅在当前节点是 Leader 时运行核心调度）
	// 简化：非 Leader 节点只运行 informer 同步，不跑 reconciler 主循环。
	leaderElector := election.NewLeaderElector(etcdCli, "/dolphin/scheduler/leader",
		hostname(), cfg.Election.TTL)

	// leaderCtx 表示「当前任期的领导权」。失去领导权时 cancel，立即停止
	// reconciler / 到期扫描 / worker 监控，避免失权节点继续调度或误判故障。
	var (
		leaderMu     sync.Mutex
		leaderCancel context.CancelFunc
	)

	// 发布给 Worker 的 Leader 地址：环境变量 DOLPHIN_ADVERTISE_ADDR > 配置
	// server.advertise_addr > 默认 localhost:<grpc_port>。
	// K8s 场景用 Downward API 注入 Pod IP（见 deployments/k8s/scheduler.yaml），
	// 否则 Worker 连 localhost 会指向自己所在的 Pod。
	advertiseAddr := os.Getenv("DOLPHIN_ADVERTISE_ADDR")
	if advertiseAddr == "" {
		advertiseAddr = cfg.Server.AdvertiseAddr
	}
	if advertiseAddr == "" {
		advertiseAddr = fmt.Sprintf("localhost:%d", cfg.Server.GRPCPort)
	}

	publishLeaderAddr := func(ctx context.Context) {
		grpcAddr := advertiseAddr
		if err := leaderElector.Publish(ctx, election.LeaderAddrKey, grpcAddr); err != nil {
			slog.Warn("publish leader addr failed", "err", err)
		} else {
			slog.Info("published leader addr", "key", election.LeaderAddrKey, "addr", grpcAddr)
		}
	}

	leaderElector.SetOnBecomeLeader(func(ctx context.Context) {
		slog.Info("starting reconciler as leader")
		lctx, lcancel := context.WithCancel(ctx)
		leaderMu.Lock()
		leaderCancel = lcancel
		leaderMu.Unlock()

		go recon.Run(lctx)
		go recon.RunDueTaskScanner(lctx, time.Second)
		// Worker 心跳超时检测 + 故障转移。检测周期 = 心跳间隔，容忍阈值 = failover.heartbeat_timeout
		heartbeatTimeout := cfg.Failover.HeartbeatTimeout
		if heartbeatTimeout <= 0 {
			heartbeatTimeout = 30 * time.Second
		}
		go recon.RunWorkerMonitor(lctx, heartbeatTimeout, 5*time.Second)

		// 执行级重试：把 Leader ctx 注入 Reconciler（失权时重试定时器随之下线），
		// 配置退避基线与 stale running 兜底阈值。
		recon.SetLeaderCtx(lctx)
		retryBase := cfg.Failover.RetryBaseDelay
		if retryBase <= 0 {
			retryBase = 2 * time.Second
		}
		recon.SetRetryPolicy(retryBase)
		staleGrace := cfg.Failover.StaleRunningGrace
		if staleGrace <= 0 {
			staleGrace = 30 * time.Second
		}
		go recon.RunStaleRunningScanner(lctx, 5*time.Second, staleGrace)

		// 将本实例的 gRPC 地址发布到 etcd，并绑定到选主 lease。
		// 立即发布一次 + 每 5s 重发布：
		//   - 立即发布：Worker 秒级发现新 Leader
		//   - 周期重发布：即使前一个 Leader 的旧地址仍残留（其 lease 尚未过期），
		//     新 Leader 的写入也会立即覆盖，保证 key 始终指向当前 Leader。
		publishLeaderAddr(lctx)
		go func() {
			t := time.NewTicker(5 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-lctx.Done():
					return
				case <-t.C:
					publishLeaderAddr(lctx)
				}
			}
		}()
	})
	leaderElector.SetOnLoseLeader(func() {
		slog.Warn("lost leadership, stopping leader-only loops")
		leaderMu.Lock()
		cancel := leaderCancel
		leaderCancel = nil
		leaderMu.Unlock()
		if cancel != nil {
			cancel()
		}
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
				// 内存注册表 worker 数：反映"本实例真正能下发任务的 worker"。
				// 故障转移测试用它判断 worker 是否已重连到本实例。
				metrics.SchedulerWorkersOnline.Set(float64(svc.WorkersOnline()))
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
			case "/debug/sleep":
				// 调试用慢端点：让测试能构造"运行中"的任务，用于验证 Worker 故障转移。
				// ?seconds=N 控制睡眠秒数，上限 120s，缺省立即返回。
				secs, _ := strconv.Atoi(r.URL.Query().Get("seconds"))
				if secs < 0 {
					secs = 0
				}
				if secs > 120 {
					secs = 120
				}
				if secs > 0 {
					time.Sleep(time.Duration(secs) * time.Second)
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(fmt.Sprintf("slept %ds", secs)))
			case "/debug/fail":
				// 调试用失败端点：返回 500，用于验证执行级重试
				// （worker 执行到 HTTP >= 400 判失败 → 触发重试直到 max_retries 耗尽）。
				// ?times=N 可选：前 N 次失败、之后成功（验证"重试后最终成功"路径）。
				times, _ := strconv.Atoi(r.URL.Query().Get("times"))
				if times <= 0 {
					times = 1 << 30 // 一直失败
				}
				cntAny, _ := failCounts.LoadOrStore(r.URL.RequestURI(), 0)
				cnt := cntAny.(int)
				if cnt < times {
					failCounts.Store(r.URL.RequestURI(), cnt+1)
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte(fmt.Sprintf("synthetic failure #%d", cnt+1)))
					return
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("recovered"))
			case "/debug/reset-fail":
				// 清空 /debug/fail 的失败计数器。测试脚本每轮开始前调用，
				// 避免上一轮的失败计数残留导致"前 N 次失败"语义串轮次
				// （如 scenario B 在重跑时第一次请求就直接 recovered）。
				failCounts.Range(func(k, _ any) bool {
					failCounts.Delete(k)
					return true
				})
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("fail counters reset"))
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
