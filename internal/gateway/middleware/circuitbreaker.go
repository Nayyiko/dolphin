package middleware

import (
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/yourname/dolphin/internal/gateway/router"
)

// ErrCircuitOpen 熔断器打开，请求被拒绝。
var ErrCircuitOpen = errors.New("circuit breaker open")

// State 熔断器状态。
type State int

const (
	// StateClosed 正常通行，累积失败。
	StateClosed State = iota
	// StateOpen 熔断，直接拒绝请求。
	StateOpen
	// StateHalfOpen 半开，放行少量探测请求。
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// CircuitBreaker 三态熔断器。
// 状态机:
//   Closed --连续失败>=threshold--> Open
//   Open   --timeout 到期--> HalfOpen
//   HalfOpen --探测成功数>=halfOpenMax--> Closed
//   HalfOpen --任一探测失败--> Open
type CircuitBreaker struct {
	state            State
	failureCount     int
	failureThreshold int
	timeout          time.Duration
	halfOpenMax      int
	halfOpenCount    int    // 半开期间已允许的探测数
	halfOpenSuccess  int    // 半开期间已成功的探测数
	lastFailureAt    time.Time

	mu sync.RWMutex
}

// CircuitBreakerConfig 熔断器配置。
type CircuitBreakerConfig struct {
	FailureThreshold int           `yaml:"failure_threshold"`
	Timeout          time.Duration `yaml:"timeout"`
	HalfOpenMax      int           `yaml:"half_open_max"`
}

// NewCircuitBreaker 创建熔断器。
func NewCircuitBreaker(cfg CircuitBreakerConfig) *CircuitBreaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.HalfOpenMax <= 0 {
		cfg.HalfOpenMax = 3
	}
	return &CircuitBreaker{
		state:            StateClosed,
		failureThreshold: cfg.FailureThreshold,
		timeout:          cfg.Timeout,
		halfOpenMax:      cfg.HalfOpenMax,
	}
}

// State 返回当前状态（线程安全）。
func (cb *CircuitBreaker) State() State {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// Allow 检查是否允许请求通过。
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateOpen:
		if time.Since(cb.lastFailureAt) > cb.timeout {
			// 冷却期结束，进入半开
			cb.state = StateHalfOpen
			cb.halfOpenCount = 0
			cb.halfOpenSuccess = 0
			return true
		}
		return false
	case StateHalfOpen:
		if cb.halfOpenCount >= cb.halfOpenMax {
			return false
		}
		cb.halfOpenCount++
		return true
	default:
		return true
	}
}

// Report 报告一次调用结果。
func (cb *CircuitBreaker) Report(success bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if success {
		cb.failureCount = 0
		if cb.state == StateHalfOpen {
			// 半开探测成功数达到阈值 → 恢复 Closed
			cb.halfOpenSuccess++
			if cb.halfOpenSuccess >= cb.halfOpenMax {
				cb.state = StateClosed
				cb.halfOpenCount = 0
				cb.halfOpenSuccess = 0
			}
		}
		return
	}

	// 失败
	cb.failureCount++
	cb.lastFailureAt = time.Now()
	switch cb.state {
	case StateClosed:
		if cb.failureCount >= cb.failureThreshold {
			cb.state = StateOpen
		}
	case StateHalfOpen:
		// 半开探测失败 → 立即回退到 Open
		cb.state = StateOpen
		cb.halfOpenCount = 0
		cb.halfOpenSuccess = 0
	}
}

// Call 执行 fn 并自动报告结果。若熔断器打开则返回 ErrCircuitOpen。
func (cb *CircuitBreaker) Call(fn func() error) error {
	if !cb.Allow() {
		return ErrCircuitOpen
	}
	err := fn()
	cb.Report(err == nil)
	return err
}

// CircuitBreakerMiddleware 熔断中间件。
// 将请求包装进熔断器。downstream func 是实际转发逻辑。
func CircuitBreakerMiddleware(cb *CircuitBreaker) router.Middleware {
	return func(next router.HandlerFunc) router.HandlerFunc {
		return func(c *router.Context) {
			err := cb.Call(func() error {
				next(c)
				if c.Status >= 500 {
					return errors.New(http.StatusText(c.Status))
				}
				return nil
			})
			if err == ErrCircuitOpen {
				c.JSON(http.StatusServiceUnavailable, map[string]any{
					"code":    "circuit_open",
					"message": "circuit breaker is open, service temporarily unavailable",
				})
				c.Abort()
			}
		}
	}
}
