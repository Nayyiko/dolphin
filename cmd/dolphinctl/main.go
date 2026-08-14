// dolphinctl 是 Dolphin 的命令行管理工具。
// 通过 gRPC 连接 scheduler，提供任务管理和压测辅助功能。
//
// 用法:
//
//	dolphinctl --addr localhost:50051 task create --name xxx --cron "*/5 * * * *" --handler http://x
//	dolphinctl --addr localhost:50051 task list
//	dolphinctl --addr localhost:50051 task trigger --id <id>
//	dolphinctl --addr localhost:50051 task logs --id <id>
//	dolphinctl --addr localhost:50051 stress create --count 100 --cron "*/1 * * * *"   # 批量创建
//	dolphinctl --addr localhost:50051 stress trigger --id <id> --count 1000             # 批量手动触发
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/yourname/dolphin/api/proto/pb"
)

var (
	addr    string
	timeout time.Duration
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	// 全局 flags
	root := flag.NewFlagSet("dolphinctl", flag.ExitOnError)
	root.StringVar(&addr, "addr", "localhost:50051", "scheduler gRPC address")
	root.DurationVar(&timeout, "timeout", 10*time.Second, "request timeout")
	root.Usage = func() {
		fmt.Println(`dolphinctl — Dolphin 命令行管理工具

用法:
  dolphinctl [全局参数] <command> [子命令参数]

全局参数:
  --addr <host:port>    scheduler gRPC 地址 (默认 localhost:50051)
  --timeout <duration>  请求超时 (默认 10s)

命令:
  task create --name <n> --cron <expr> --handler <url> [--type http|shell] [--params <json>] [--timeout <sec>] [--retries <n>]
  task list [--status <active|paused>] [--limit <n>]
  task get --id <id>
  task trigger --id <id>
  task pause --id <id>
  task resume --id <id>
  task delete --id <id>
  task logs --id <id> [--limit <n>]
  stress create --count <n> --prefix <name> --cron <expr> --handler <url>
  stress trigger --id <id> --count <n>
  version`)
	}
	_ = root.Parse(os.Args[1:])

	if len(root.Args()) < 1 {
		root.Usage()
		os.Exit(1)
	}

	cmd := root.Args()[0]
	args := root.Args()[1:]

	switch cmd {
	case "task":
		runTask(args)
	case "stress":
		runStress(args)
	case "version":
		fmt.Println("dolphinctl dev (github.com/yourname/dolphin)")
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", cmd)
		root.Usage()
		os.Exit(1)
	}
}

func newClient(ctx context.Context) (pb.SchedulerClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("connect %s: %w", addr, err)
	}
	return pb.NewSchedulerClient(conn), conn, nil
}

// ==================== task 子命令 ====================

func runTask(args []string) {
	fs := flag.NewFlagSet("task", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Println(`task 子命令:
  create --name <n> --cron <expr> --handler <url> [--type http|shell] [--params <json>] [--timeout <sec>] [--retries <n>] [--depend-on <id1,id2>] [--dep-policy all_success|all_completed]
  update --id <id> --name <n> --cron <expr> --handler <url> [--type http|shell] [--depend-on <id1,id2>] [--dep-policy all_success|all_completed]
  list [--status <status>] [--limit <n>]
  get --id <id>
  trigger --id <id>
  trigger-batch --ids <id1,id2,...>   # 进程内批量触发（并发压测用）
  pause --id <id>
  resume --id <id>
  delete --id <id>
  logs --id <id> [--limit <n>]`)
	}
	if len(args) < 1 {
		fs.Usage()
		os.Exit(1)
	}
	sub := args[0]
	subArgs := args[1:]

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	client, conn, err := newClient(ctx)
	if err != nil {
		slog.Error("connect failed", "err", err)
		os.Exit(1)
	}
	defer conn.Close()

	switch sub {
	case "create":
		var name, cronExpr, handler, handlerType, params string
		var dependOn string
		var depPolicy string
		var taskTimeout, retries int
		fs := flag.NewFlagSet("create", flag.ExitOnError)
		fs.StringVar(&name, "name", "", "task name")
		fs.StringVar(&cronExpr, "cron", "", "cron expression (5 fields)")
		fs.StringVar(&handler, "handler", "", "handler URL or command")
		fs.StringVar(&handlerType, "type", "http", "handler type: http/shell")
		fs.StringVar(&params, "params", "", "params JSON")
		fs.IntVar(&taskTimeout, "timeout", 30, "timeout seconds")
		fs.IntVar(&retries, "retries", 3, "max retries")
		fs.StringVar(&dependOn, "depend-on", "", "upstream task IDs, comma separated (DAG)")
		fs.StringVar(&depPolicy, "dep-policy", "all_success", "dependency policy: all_success/all_completed")
		_ = fs.Parse(subArgs)

		if name == "" || cronExpr == "" || handler == "" {
			slog.Error("name, cron, handler are required")
			fs.Usage()
			os.Exit(1)
		}
		resp, err := client.CreateTask(ctx, &pb.CreateTaskRequest{
			Name:        name,
			CronExpr:    cronExpr,
			Handler:     handler,
			HandlerType: handlerType,
			Params:      params,
			Timeout:     int32(taskTimeout),
			MaxRetries:  int32(retries),
			DependOn:    splitCSV(dependOn),
			DepPolicy:   depPolicy,
		})
		if err != nil {
			slog.Error("create task failed", "err", err)
			os.Exit(1)
		}
		printTask(resp)

	case "update":
		var id, name, cronExpr, handler, handlerType, params string
		var dependOn string
		var depPolicy string
		var taskTimeout, retries int
		fs := flag.NewFlagSet("update", flag.ExitOnError)
		fs.StringVar(&id, "id", "", "task id")
		fs.StringVar(&name, "name", "", "task name")
		fs.StringVar(&cronExpr, "cron", "", "cron expression (5 fields)")
		fs.StringVar(&handler, "handler", "", "handler URL or command")
		fs.StringVar(&handlerType, "type", "http", "handler type: http/shell")
		fs.StringVar(&params, "params", "", "params JSON")
		fs.IntVar(&taskTimeout, "timeout", 30, "timeout seconds")
		fs.IntVar(&retries, "retries", 3, "max retries")
		fs.StringVar(&dependOn, "depend-on", "", "upstream task IDs, comma separated (DAG)")
		fs.StringVar(&depPolicy, "dep-policy", "all_success", "dependency policy: all_success/all_completed")
		_ = fs.Parse(subArgs)
		if id == "" {
			slog.Error("--id required")
			fs.Usage()
			os.Exit(1)
		}
		resp, err := client.UpdateTask(ctx, &pb.UpdateTaskRequest{
			Id:          id,
			Name:        name,
			CronExpr:    cronExpr,
			Handler:     handler,
			HandlerType: handlerType,
			Params:      params,
			Timeout:     int32(taskTimeout),
			MaxRetries:  int32(retries),
			DependOn:    splitCSV(dependOn),
			DepPolicy:   depPolicy,
		})
		if err != nil {
			slog.Error("update task failed", "err", err)
			os.Exit(1)
		}
		printTask(resp)

	case "list":
		var status string
		var limit int
		fs := flag.NewFlagSet("list", flag.ExitOnError)
		fs.StringVar(&status, "status", "", "filter by status")
		fs.IntVar(&limit, "limit", 20, "max results")
		_ = fs.Parse(subArgs)

		resp, err := client.ListTasks(ctx, &pb.ListTasksRequest{Status: status, Limit: int32(limit)})
		if err != nil {
			slog.Error("list tasks failed", "err", err)
			os.Exit(1)
		}
		fmt.Printf("total: %d\n", resp.Total)
		for _, t := range resp.Tasks {
			printTask(t)
		}

	case "get":
		var id string
		fs := flag.NewFlagSet("get", flag.ExitOnError)
		fs.StringVar(&id, "id", "", "task id")
		_ = fs.Parse(subArgs)
		if id == "" {
			slog.Error("--id required")
			os.Exit(1)
		}
		resp, err := client.GetTask(ctx, &pb.GetTaskRequest{Id: id})
		if err != nil {
			slog.Error("get task failed", "err", err)
			os.Exit(1)
		}
		printTask(resp)

	case "trigger":
		var id string
		fs := flag.NewFlagSet("trigger", flag.ExitOnError)
		fs.StringVar(&id, "id", "", "task id")
		_ = fs.Parse(subArgs)
		if id == "" {
			slog.Error("--id required")
			os.Exit(1)
		}
		if _, err := client.TriggerTask(ctx, &pb.TriggerTaskRequest{Id: id}); err != nil {
			slog.Error("trigger failed", "err", err)
			os.Exit(1)
		}
		fmt.Println("triggered")

	case "trigger-batch":
		// 进程内批量触发：一次进程发 N 个 gRPC TriggerTask，制造同时到期突发。
		// 相比逐条 dolphinctl 调用（每次新进程 ~0.2s），把触发耗时从 O(N) 降到 ~O(N)/1000，
		// 让 N 个任务几乎同时进入调度队列，测真实的调度/执行并发。
		var ids string
		fs := flag.NewFlagSet("trigger-batch", flag.ExitOnError)
		fs.StringVar(&ids, "ids", "", "comma-separated task ids")
		_ = fs.Parse(subArgs)
		if ids == "" {
			slog.Error("--ids required (comma-separated)")
			os.Exit(1)
		}
		idList := splitCSV(ids)
		start := time.Now()
		ok := 0
		for _, id := range idList {
			if _, err := client.TriggerTask(ctx, &pb.TriggerTaskRequest{Id: id}); err != nil {
				slog.Warn("trigger failed", "id", id, "err", err)
				continue
			}
			ok++
		}
		fmt.Printf("triggered %d/%d in %s\n", ok, len(idList), time.Since(start).Round(time.Millisecond))

	case "pause":
		var id string
		fs := flag.NewFlagSet("pause", flag.ExitOnError)
		fs.StringVar(&id, "id", "", "task id")
		_ = fs.Parse(subArgs)
		if id == "" {
			slog.Error("--id required")
			os.Exit(1)
		}
		resp, err := client.PauseTask(ctx, &pb.PauseTaskRequest{Id: id})
		if err != nil {
			slog.Error("pause failed", "err", err)
			os.Exit(1)
		}
		printTask(resp)

	case "resume":
		var id string
		fs := flag.NewFlagSet("resume", flag.ExitOnError)
		fs.StringVar(&id, "id", "", "task id")
		_ = fs.Parse(subArgs)
		if id == "" {
			slog.Error("--id required")
			os.Exit(1)
		}
		resp, err := client.ResumeTask(ctx, &pb.ResumeTaskRequest{Id: id})
		if err != nil {
			slog.Error("resume failed", "err", err)
			os.Exit(1)
		}
		printTask(resp)

	case "delete":
		var id string
		fs := flag.NewFlagSet("delete", flag.ExitOnError)
		fs.StringVar(&id, "id", "", "task id")
		_ = fs.Parse(subArgs)
		if id == "" {
			slog.Error("--id required")
			os.Exit(1)
		}
		if _, err := client.DeleteTask(ctx, &pb.DeleteTaskRequest{Id: id}); err != nil {
			slog.Error("delete failed", "err", err)
			os.Exit(1)
		}
		fmt.Println("deleted")

	case "logs":
		var id string
		var limit int
		fs := flag.NewFlagSet("logs", flag.ExitOnError)
		fs.StringVar(&id, "id", "", "task id")
		fs.IntVar(&limit, "limit", 20, "max logs")
		_ = fs.Parse(subArgs)
		if id == "" {
			slog.Error("--id required")
			os.Exit(1)
		}
		resp, err := client.GetTaskLogs(ctx, &pb.GetTaskLogsRequest{TaskId: id, Limit: int32(limit)})
		if err != nil {
			slog.Error("get logs failed", "err", err)
			os.Exit(1)
		}
		if len(resp.Logs) == 0 {
			fmt.Println("no logs")
			return
		}
		fmt.Printf("%-38s %-12s %-16s %-10s %s\n", "INSTANCE", "STATUS", "WORKER", "RETRIES", "TIME")
		for _, l := range resp.Logs {
			fmt.Printf("%-38s %-12s %-16s %-10d %s\n",
				l.InstanceId, l.Status, l.WorkerId, l.RetryCount,
				time.Unix(l.StartTime, 0).Format("15:04:05"))
		}

	default:
		fmt.Fprintf(os.Stderr, "unknown task subcommand: %s\n", sub)
		os.Exit(1)
	}
}

func printTask(t *pb.Task) {
	deps := "[]"
	if len(t.DependOn) > 0 {
		deps = fmt.Sprintf("%v", t.DependOn)
	}
	fmt.Printf("id=%s status=%s name=%s cron=%q next=%s type=%s deps=%s policy=%s\n",
		t.Id, t.Status, t.Name, t.CronExpr,
		time.Unix(t.NextRunAt, 0).Format("2006-01-02 15:04:05"),
		t.HandlerType, deps, t.DepPolicy)
}

// splitCSV 将逗号分隔的字符串拆成列表，空串返回 nil。
func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// ==================== stress 子命令 ====================

func runStress(args []string) {
	fs := flag.NewFlagSet("stress", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Println(`stress 子命令:
  create --count <n> --prefix <name> --cron <expr> --handler <url> [--type http|shell] [--timeout <sec>]
  trigger --id <id> --count <n>   # 手动触发 n 次（测调度吞吐/延迟）`)
	}
	if len(args) < 1 {
		fs.Usage()
		os.Exit(1)
	}
	sub := args[0]
	subArgs := args[1:]

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client, conn, err := newClient(ctx)
	if err != nil {
		slog.Error("connect failed", "err", err)
		os.Exit(1)
	}
	defer conn.Close()

	switch sub {
	case "create":
		var count int
		var prefix, cronExpr, handler, handlerType string
		var taskTimeout int
		fs := flag.NewFlagSet("create", flag.ExitOnError)
		fs.IntVar(&count, "count", 100, "number of tasks to create")
		fs.StringVar(&prefix, "prefix", "stress", "task name prefix")
		fs.StringVar(&cronExpr, "cron", "*/5 * * * *", "cron expression")
		fs.StringVar(&handler, "handler", "http://localhost:9090/healthz", "handler URL")
		fs.StringVar(&handlerType, "type", "http", "handler type")
		fs.IntVar(&taskTimeout, "timeout", 10, "timeout seconds")
		_ = fs.Parse(subArgs)
		if count <= 0 {
			slog.Error("--count must be > 0")
			os.Exit(1)
		}

		start := time.Now()
		success := 0
		for i := 0; i < count; i++ {
			name := fmt.Sprintf("%s-%05d", prefix, i)
			_, err := client.CreateTask(ctx, &pb.CreateTaskRequest{
				Name:        name,
				CronExpr:    cronExpr,
				Handler:     handler,
				HandlerType: handlerType,
				Timeout:     int32(taskTimeout),
				MaxRetries:  3,
			})
			if err != nil {
				slog.Warn("create failed", "i", i, "err", err)
				continue
			}
			success++
			if i > 0 && i%20 == 0 {
				fmt.Printf("  created %d/%d\n", i, count)
			}
		}
		elapsed := time.Since(start)
		fmt.Printf("created %d/%d tasks in %s (%.1f tasks/sec)\n",
			success, count, elapsed.Round(time.Millisecond),
			float64(success)/elapsed.Seconds())

	case "trigger":
		var id string
		var count int
		fs := flag.NewFlagSet("trigger", flag.ExitOnError)
		fs.StringVar(&id, "id", "", "task id")
		fs.IntVar(&count, "count", 100, "times to trigger")
		_ = fs.Parse(subArgs)
		if id == "" || count <= 0 {
			slog.Error("--id and --count required (count > 0)")
			os.Exit(1)
		}

		start := time.Now()
		success := 0
		for i := 0; i < count; i++ {
			if _, err := client.TriggerTask(ctx, &pb.TriggerTaskRequest{Id: id}); err != nil {
				slog.Warn("trigger failed", "i", i, "err", err)
				continue
			}
			success++
		}
		elapsed := time.Since(start)
		fmt.Printf("triggered %d/%d in %s (%.1f trigger/sec)\n",
			success, count, elapsed.Round(time.Millisecond),
			float64(success)/elapsed.Seconds())

	default:
		fmt.Fprintf(os.Stderr, "unknown stress subcommand: %s\n", sub)
		os.Exit(1)
	}
}
