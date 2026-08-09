package cron

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// fieldSpec 表示 cron 字段的合法值集合。
type fieldSpec struct {
	values map[int]bool // 显式列出的值（也存储展开后的范围值）
}

// matches 判断 value 是否命中该字段。
func (f *fieldSpec) matches(value int) bool {
	return f.values[value]
}

// newFieldSpec 解析单个 cron 字段表达式（如 "0,15,30,45"、"*/5"、"1-10"、"*"）。
// min/max 为该字段的合法范围（分钟 0-59，小时 0-23 等）。
func newFieldSpec(expr string, min, max int) (*fieldSpec, error) {
	fs := &fieldSpec{values: make(map[int]bool)}

	parts := strings.Split(expr, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("empty field part in %q", expr)
		}

		var start, end, step int

		// 步进 "*/n" 或 "a-b/n"
		step = 1
		if idx := strings.Index(part, "/"); idx >= 0 {
			s, err := strconv.Atoi(part[idx+1:])
			if err != nil || s <= 0 {
				return nil, fmt.Errorf("invalid step in %q", part)
			}
			step = s
			part = part[:idx]
		}

		if part == "*" {
			start, end = min, max
		} else if idx := strings.Index(part, "-"); idx >= 0 {
			a, err1 := strconv.Atoi(part[:idx])
			b, err2 := strconv.Atoi(part[idx+1:])
			if err1 != nil || err2 != nil {
				return nil, fmt.Errorf("invalid range in %q", part)
			}
			start, end = a, b
		} else {
			v, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("invalid value in %q", part)
			}
			start, end = v, v
		}

		if start < min || end > max || start > end {
			return nil, fmt.Errorf("value out of range [%d-%d] in %q", min, max, part)
		}

		for v := start; v <= end; v += step {
			fs.values[v] = true
		}
	}
	return fs, nil
}

// CronSchedule 完整的 5-field cron 表达式。
type CronSchedule struct {
	Minute     *fieldSpec
	Hour       *fieldSpec
	DayOfMonth *fieldSpec
	Month      *fieldSpec
	DayOfWeek  *fieldSpec
}

// Parse 解析标准 5-field cron 表达式。
// 格式: minute hour dayOfMonth month dayOfWeek
// 支持 *、*/n、a-b、a-b/n、a,b,c 及组合。
func Parse(expr string) (*CronSchedule, error) {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, fmt.Errorf("cron expression must have 5 fields, got %d: %q", len(fields), expr)
	}

	minute, err := newFieldSpec(fields[0], 0, 59)
	if err != nil {
		return nil, fmt.Errorf("minute: %w", err)
	}
	hour, err := newFieldSpec(fields[1], 0, 23)
	if err != nil {
		return nil, fmt.Errorf("hour: %w", err)
	}
	dayOfMonth, err := newFieldSpec(fields[2], 1, 31)
	if err != nil {
		return nil, fmt.Errorf("day of month: %w", err)
	}
	month, err := newFieldSpec(fields[3], 1, 12)
	if err != nil {
		return nil, fmt.Errorf("month: %w", err)
	}
	dayOfWeek, err := newFieldSpec(fields[4], 0, 6) // 0=Sunday
	if err != nil {
		return nil, fmt.Errorf("day of week: %w", err)
	}

	return &CronSchedule{
		Minute:     minute,
		Hour:       hour,
		DayOfMonth: dayOfMonth,
		Month:      month,
		DayOfWeek:  dayOfWeek,
	}, nil
}

// MustParse 解析失败时 panic。仅用于测试或配置固定的场景。
func MustParse(expr string) *CronSchedule {
	s, err := Parse(expr)
	if err != nil {
		panic(err)
	}
	return s
}

// Next 计算 t 之后的下一次触发时间（不含 t 本身，秒清零）。
// 核心算法：按月→日→时→分逐级推进，遇到不匹配的维度跳到该维度的下一合法值。
// 循环上界 5 年，防止无效表达式导致死循环。
func (cs *CronSchedule) Next(t time.Time) time.Time {
	t = t.Truncate(time.Minute).Add(time.Minute)

	// 最多推进 5 年（以分钟计）
	limit := 5 * 365 * 24 * 60
	for i := 0; i < limit; i++ {
		if !cs.Month.matches(int(t.Month())) {
			// 跳到下月 1 号 0 点
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, t.Location())
			continue
		}
		if !cs.DayOfMonth.matches(t.Day()) || !cs.DayOfWeek.matches(int(t.Weekday())) {
			// 跳到次日 0 点
			t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
			t = t.AddDate(0, 0, 1)
			continue
		}
		if !cs.Hour.matches(t.Hour()) {
			// 跳到下一小时 0 分
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
			t = t.Add(time.Hour)
			continue
		}
		if !cs.Minute.matches(t.Minute()) {
			t = t.Add(time.Minute)
			continue
		}
		return t
	}
	return time.Time{} // 未找到匹配
}
