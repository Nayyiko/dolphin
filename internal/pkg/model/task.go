package model

import (
	"time"

	"gorm.io/gorm"
)

// Task 任务定义。
// 对应 Kubernetes 中的 "Spec" —— 用户声明的期望状态。
// 采用软删除（gorm.DeletedAt），避免误删导致调度中的任务丢失。
type Task struct {
	ID        string `gorm:"primaryKey;type:varchar(36)" json:"id"`
	Name      string `gorm:"type:varchar(255);not null" json:"name"`
	CronExpr  string `gorm:"type:varchar(100);not null" json:"cronExpr"`
	// NextRunAt 下一次触发时间（预计算，调度器依据此字段扫描到期任务）。
	NextRunAt time.Time `gorm:"not null;index:idx_status_next,priority:2" json:"nextRunAt"`
	// Handler 执行目标。http/grpc 为 URL/地址，shell 为命令。
	Handler string `gorm:"type:varchar(500);not null" json:"handler"`
	// HandlerType 执行方式: http / grpc / shell。
	HandlerType string `gorm:"type:varchar(20);not null;default:'http'" json:"handlerType"`
	// Params 执行参数，JSON 字符串。
	Params string `gorm:"type:text" json:"params"`
	// Timeout 单次执行超时（秒）。
	Timeout int `gorm:"not null;default:30" json:"timeout"`
	// MaxRetries 最大重试次数。
	MaxRetries int `gorm:"not null;default:3" json:"maxRetries"`
	// Status: active / paused / deleted（软删前的标记）。
	Status string `gorm:"type:varchar(20);not null;default:'active';index:idx_status_next,priority:1" json:"status"`

	// DependOn 上游任务 ID 列表（JSON 数组字符串，如 `["a","b"]`）。
	// 非空时本任务构成 DAG 的依赖节点：只有依赖满足后才被调度。
	// 依赖语义（task-level dependency）：
	//   - all_success  : 每个上游在"本任务上次运行之后"至少有一次成功的执行。
	//   - all_completed: 每个上游在"本任务上次运行之后"至少有一次完成的执行（success/failed/timeout）。
	// "本任务上次运行之后" 保证依赖按新鲜度匹配，不会串到上一次运行的旧结果。
	DependOn string `gorm:"type:text" json:"dependOn"`
	// DepPolicy 依赖策略: all_success（默认）/ all_completed。
	DepPolicy string `gorm:"type:varchar(20);not null;default:'all_success'" json:"depPolicy"`

	// DeletionTimestamp 请求删除的时间。非空时由 Reconciler 执行优雅清理。
	DeletionTimestamp *time.Time `gorm:"index" json:"deletionTimestamp,omitempty"`

	CreatedAt time.Time      `gorm:"not null" json:"createdAt"`
	UpdatedAt time.Time      `gorm:"not null" json:"updatedAt"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"-"`
}

// TaskStatusValues 合法的任务状态。
const (
	TaskStatusActive  = "active"
	TaskStatusPaused  = "paused"
	TaskStatusDeleted = "deleted"
)

// 依赖策略值。
const (
	// DepPolicyAllSuccess 所有上游在本次调度之后成功，才允许调度本任务。
	DepPolicyAllSuccess = "all_success"
	// DepPolicyAllCompleted 所有上游在本次调度之后完成（成功/失败/超时均可）。
	DepPolicyAllCompleted = "all_completed"
)

// DepPolicyValues 合法的依赖策略。
var DepPolicyValues = map[string]bool{
	DepPolicyAllSuccess:  true,
	DepPolicyAllCompleted: true,
}

// HandlerTypeValues 合法的执行方式。
const (
	HandlerTypeHTTP  = "http"
	HandlerTypeGRPC  = "grpc"
	HandlerTypeShell = "shell"
)
