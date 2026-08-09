package cron

import (
	"testing"
	"time"
)

func loc() *time.Location { return time.UTC }

func mustParse(t testing.TB, expr string) *CronSchedule {
	t.Helper()
	s, err := Parse(expr)
	if err != nil {
		t.Fatalf("parse %q: %v", expr, err)
	}
	return s
}

func TestParse_Valid(t *testing.T) {
	exprs := []string{
		"*/5 * * * *",
		"0 0 * * *",
		"0 2 * * 1",
		"30 8 * * 1-5",
		"0,15,30,45 * * * *",
		"0 9-18 * * *",
		"0 0 1 * *",
	}
	for _, e := range exprs {
		if _, err := Parse(e); err != nil {
			t.Errorf("expected %q valid, got err %v", e, err)
		}
	}
}

func TestParse_Invalid(t *testing.T) {
	exprs := []string{
		"* * * *",      // 4 fields
		"60 * * * *",   // minute out of range
		"* 24 * * *",   // hour out of range
		"* * 0 * *",    // day out of range
		"* * * 13 *",   // month out of range
		"*/0 * * * *",  // step 0
		"* * * * * *",  // 6 fields
		"abc * * * *",  // not a number
	}
	for _, e := range exprs {
		if _, err := Parse(e); err == nil {
			t.Errorf("expected %q invalid, got nil err", e)
		}
	}
}

func TestNext_Every5Minutes(t *testing.T) {
	s := mustParse(t, "*/5 * * * *")
	base := time.Date(2026, 8, 9, 10, 0, 0, 0, loc())

	cases := []struct {
		from time.Time
		want time.Time
	}{
		{time.Date(2026, 8, 9, 10, 0, 0, 0, loc()), time.Date(2026, 8, 9, 10, 5, 0, 0, loc())},
		{time.Date(2026, 8, 9, 10, 3, 30, 0, loc()), time.Date(2026, 8, 9, 10, 5, 0, 0, loc())},
		{time.Date(2026, 8, 9, 10, 5, 0, 0, loc()), time.Date(2026, 8, 9, 10, 10, 0, 0, loc())},
		{time.Date(2026, 8, 9, 23, 58, 0, 0, loc()), time.Date(2026, 8, 10, 0, 0, 0, 0, loc())},
	}
	for _, tc := range cases {
		got := s.Next(tc.from)
		if !got.Equal(tc.want) {
			t.Errorf("Next(%v) = %v, want %v", tc.from, got, tc.want)
		}
	}
	_ = base
}

func TestNext_DailyMidnight(t *testing.T) {
	s := mustParse(t, "0 0 * * *")
	got := s.Next(time.Date(2026, 8, 9, 0, 0, 0, 0, loc()))
	want := time.Date(2026, 8, 10, 0, 0, 0, 0, loc())
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestNext_WeekdayAt830(t *testing.T) {
	// 0 8 * * 1-5: 周一到周五 8:30
	s := mustParse(t, "30 8 * * 1-5")

	// 2026-08-09 是周日(Sunday=0)。下一次周一是 8-10。
	got := s.Next(time.Date(2026, 8, 9, 10, 0, 0, 0, loc()))
	want := time.Date(2026, 8, 10, 8, 30, 0, 0, loc())
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestNext_SpecificMinutes(t *testing.T) {
	s := mustParse(t, "0,30 * * * *")
	got := s.Next(time.Date(2026, 8, 9, 10, 15, 0, 0, loc()))
	want := time.Date(2026, 8, 9, 10, 30, 0, 0, loc())
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestNext_MonthlyFirstDay(t *testing.T) {
	s := mustParse(t, "0 0 1 * *")
	got := s.Next(time.Date(2026, 8, 1, 0, 0, 0, 0, loc()))
	want := time.Date(2026, 9, 1, 0, 0, 0, 0, loc())
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestNext_HourRange(t *testing.T) {
	s := mustParse(t, "0 9-18 * * *")
	// 19:00 之后 → 明天 9:00
	got := s.Next(time.Date(2026, 8, 9, 19, 0, 0, 0, loc()))
	want := time.Date(2026, 8, 10, 9, 0, 0, 0, loc())
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestNext_Weekend(t *testing.T) {
	// 只在周六(6)触发
	s := mustParse(t, "0 0 * * 6")
	// 2026-08-09 是周日 → 下一个周六 8-15
	got := s.Next(time.Date(2026, 8, 9, 10, 0, 0, 0, loc()))
	want := time.Date(2026, 8, 15, 0, 0, 0, 0, loc())
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func BenchmarkCron_Next(b *testing.B) {
	s := mustParse(b, "*/5 * * * *")
	t := time.Date(2026, 8, 9, 10, 0, 0, 0, loc())
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.Next(t)
	}
}
