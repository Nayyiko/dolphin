package middleware

import (
	"errors"
	"testing"
	"time"
)

func TestCircuitBreaker_Transitions(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 3,
		Timeout:          50 * time.Millisecond,
		HalfOpenMax:      2,
	})

	// 初始 Closed
	if cb.State() != StateClosed {
		t.Fatalf("expected closed, got %v", cb.State())
	}

	// 失败 3 次 → Open
	for i := 0; i < 3; i++ {
		cb.Report(false)
	}
	if cb.State() != StateOpen {
		t.Fatalf("expected open after 3 failures, got %v", cb.State())
	}

	// Open 状态拒绝请求
	if cb.Allow() {
		t.Fatalf("expected open to reject")
	}

	// 冷却期结束 → HalfOpen
	time.Sleep(60 * time.Millisecond)
	if !cb.Allow() {
		t.Fatalf("expected half-open to allow probe after timeout")
	}
	if cb.State() != StateHalfOpen {
		t.Fatalf("expected half-open, got %v", cb.State())
	}

	// 半开探测成功 2 次（halfOpenMax）→ Closed
	cb.Report(true)
	cb.Report(true)
	if cb.State() != StateClosed {
		t.Fatalf("expected closed after successful probes, got %v", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenFailure(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 2,
		Timeout:          30 * time.Millisecond,
		HalfOpenMax:      3,
	})

	// 打开
	cb.Report(false)
	cb.Report(false)
	if cb.State() != StateOpen {
		t.Fatalf("expected open")
	}

	// 进入半开
	time.Sleep(40 * time.Millisecond)
	if !cb.Allow() {
		t.Fatalf("expected allow in half-open")
	}

	// 半开探测失败 → 回退 Open
	cb.Report(false)
	if cb.State() != StateOpen {
		t.Fatalf("expected open after probe failure, got %v", cb.State())
	}
}

func TestCircuitBreaker_Call(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		FailureThreshold: 1,
		Timeout:          20 * time.Millisecond,
		HalfOpenMax:      1,
	})

	// 第一次失败
	err := cb.Call(func() error {
		return errors.New("boom")
	})
	if err == nil {
		t.Fatalf("expected error from fn")
	}

	// 已 Open，Call 直接返回 ErrCircuitOpen
	err = cb.Call(func() error { return nil })
	if err != ErrCircuitOpen {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
}
