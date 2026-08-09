package model

import (
	"time"
)

// TaskLog 任务执行历史。一次调度产生一条记录。
// instance_id 标识一次具体的执行，用于幂等和追溯。
type TaskLog struct {
	ID         string    `gorm:"primaryKey;type:varchar(36)" json:"id"`
	TaskID     string    `gorm:"type:varchar(36);not null;index:idx_task_id" json:"taskId"`
	InstanceID string    `gorm:"type:varchar(36);not null;index:idx_instance_id" json:"instanceId"`
	WorkerID   string    `gorm:"type:varchar(64);not null" json:"workerId"`
	// Status: running / success / failed / timeout / cancelled。
	Status    string     `gorm:"type:varchar(20);not null;default:'running'" json:"status"`
	StartTime time.Time  `gorm:"not null;index:idx_start_time" json:"startTime"`
	EndTime   *time.Time `gorm:"index" json:"endTime,omitempty"`
	Result    string     `gorm:"type:text" json:"result,omitempty"`
	ErrorMsg  string     `gorm:"type:text" json:"errorMsg,omitempty"`
	// RetryCount 第几次重试（0 表示首次执行）。
	RetryCount int `gorm:"not null;default:0" json:"retryCount"`
	CreatedAt  time.Time `gorm:"not null" json:"createdAt"`
}

// TaskLog 状态值。
const (
	TaskLogStatusRunning   = "running"
	TaskLogStatusSuccess   = "success"
	TaskLogStatusFailed    = "failed"
	TaskLogStatusTimeout   = "timeout"
	TaskLogStatusCancelled = "cancelled"
)
