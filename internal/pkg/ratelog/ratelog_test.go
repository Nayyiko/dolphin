package ratelog

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// capture 切换默认 slog 输出到 buffer，返回恢复函数。
func capture() (*bytes.Buffer, func()) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	return &buf, func() { slog.SetDefault(old) }
}

func TestRateLimitedWithinInterval(t *testing.T) {
	buf, restore := capture()
	defer restore()

	l := New(50 * time.Millisecond)
	l.Info("first")
	l.Info("second") // 在 interval 内，应被丢弃
	l.Warn("warn-second")
	time.Sleep(60 * time.Millisecond)
	l.Info("third") // 超过 interval，应输出并附带 dropped=2

	out := buf.String()
	for _, want := range []string{"first", "third"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in output:\n%s", want, out)
		}
	}
	for _, drop := range []string{"second", "warn-second"} {
		if strings.Contains(out, drop) {
			t.Fatalf("%q should have been rate-limited:\n%s", drop, out)
		}
	}
	if !strings.Contains(out, "dropped=2") {
		t.Fatalf("expected dropped=2 (second + warn-second) in output:\n%s", out)
	}
}

func TestLogsAfterIntervalElapses(t *testing.T) {
	buf, restore := capture()
	defer restore()

	l := New(10 * time.Millisecond)
	l.Info("a")
	time.Sleep(20 * time.Millisecond)
	l.Info("b")
	time.Sleep(20 * time.Millisecond)
	l.Info("c")

	out := buf.String()
	for _, s := range []string{"a", "b", "c"} {
		if !strings.Contains(out, s) {
			t.Fatalf("missing %q in output:\n%s", s, out)
		}
	}
}
