// Package ratelog 提供限频日志器。
//
// 背景：高吞吐路径上"每事件同步写一条 slog"会成为瓶颈——Go 写慢速
// Windows 控制台时会阻塞当前 goroutine（控制台内部渲染/锁竞争），
// 拖垮业务吞吐。真实案例：
//   - gateway：每请求同步 slog 写控制台，压测时 handler 被阻塞导致响应
//     超时/连接错误；改为 100ms 限频后吞吐提升 4 倍。
//   - scheduler：gRPC recv 循环对每条 TaskResult 同步 slog.Info，单 goroutine
//     串行处理 170 个结果时被控制台渲染拖到 ~180s；改为限频后应回到秒级。
package ratelog

import (
	"log/slog"
	"sync"
	"time"
)

// Logger 限频日志器：距离上一次真正输出至少间隔 interval 才打一条。
// 被限频丢弃的日志累计计数，在下次真正输出时附带 dropped 字段，
// 保证"丢了多少"也可观测。
type Logger struct {
	mu       sync.Mutex
	interval time.Duration
	lastLog  time.Time
	dropped  uint64
}

// New 创建限频日志器。interval 内最多输出一条；interval<=0 时默认 500ms。
func New(interval time.Duration) *Logger {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	return &Logger{interval: interval}
}

// Info 限频调用 slog.Info。被限频丢弃的条数累积，在下一条真正输出时
// 以 dropped 字段上报（便于事后核对是否丢过关键日志）。
func (l *Logger) Info(msg string, args ...any) {
	l.mu.Lock()
	now := time.Now()
	if now.Sub(l.lastLog) >= l.interval {
		l.lastLog = now
		dropped := l.dropped
		l.dropped = 0
		l.mu.Unlock()
		if dropped > 0 {
			args = append(args, "dropped", dropped)
		}
		slog.Info(msg, args...)
		return
	}
	l.dropped++
	l.mu.Unlock()
}

// Warn 限频调用 slog.Warn。语义同 Info。
func (l *Logger) Warn(msg string, args ...any) {
	l.mu.Lock()
	now := time.Now()
	if now.Sub(l.lastLog) >= l.interval {
		l.lastLog = now
		dropped := l.dropped
		l.dropped = 0
		l.mu.Unlock()
		if dropped > 0 {
			args = append(args, "dropped", dropped)
		}
		slog.Warn(msg, args...)
		return
	}
	l.dropped++
	l.mu.Unlock()
}
