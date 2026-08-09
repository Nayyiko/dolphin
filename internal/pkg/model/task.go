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

// HandlerTypeValues 合法的执行方式。
const (
	HandlerTypeHTTP  = "http"
	HandlerTypeGRPC  = "grpc"
	HandlerTypeShell = "shell"
)
