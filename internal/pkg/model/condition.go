package model

import (
	"time"
)

// ConditionType 条件类型。对应 Kubernetes 中 Pod 的 Conditions。
type ConditionType string

const (
	// ConditionScheduled 本周期是否已成功调度。
	ConditionScheduled ConditionType = "Scheduled"
	// ConditionRunning 当前是否有执行中的实例。
	ConditionRunning ConditionType = "Running"
	// ConditionHealthy 近期执行是否健康（成功率/最近结果）。
	ConditionHealthy ConditionType = "Healthy"
	// ConditionReady 是否准备好接收调度（依赖 Worker 可用性等）。
	ConditionReady ConditionType = "Ready"
	// ConditionReconciling 是否正在协调中。
	ConditionReconciling ConditionType = "Reconciling"
)

// ConditionStatus 条件状态。
type ConditionStatus string

const (
	StatusTrue    ConditionStatus = "True"
	StatusFalse   ConditionStatus = "False"
	StatusUnknown ConditionStatus = "Unknown"
)

// Condition 单个条件。多维状态，每个条件独立变化。
type Condition struct {
	Type   ConditionType   `json:"type"`
	Status ConditionStatus `json:"status"`
	// Reason 机器可读原因，如 "Dispatched"、"MaxRetriesExceeded"。
	Reason string `json:"reason"`
	// Message 人类可读描述。
	Message string `json:"message"`
	// ObservedAt 上次观测时间。
	ObservedAt time.Time `json:"observedAt"`
	// TransitionAt 状态发生转换的时间。
	TransitionAt time.Time `json:"transitionAt"`
}

// TaskCondition 持久化到 MySQL 的条件记录。
type TaskCondition struct {
	ID           int64          `gorm:"primaryKey;autoIncrement" json:"id"`
	TaskID       string         `gorm:"type:varchar(36);not null;index:idx_task_type,priority:1" json:"taskId"`
	Type         ConditionType  `gorm:"type:varchar(64);not null;index:idx_task_type,priority:2" json:"type"`
	Status       ConditionStatus `gorm:"type:varchar(16);not null" json:"status"`
	Reason       string         `gorm:"type:varchar(255);not null" json:"reason"`
	Message      string         `gorm:"type:text" json:"message"`
	ObservedAt   time.Time      `gorm:"not null" json:"observedAt"`
	TransitionAt time.Time      `gorm:"not null" json:"transitionAt"`
	CreatedAt    time.Time      `gorm:"not null" json:"createdAt"`
}

// ConditionTypeValues 合法的条件类型。
var ValidConditionTypes = map[ConditionType]bool{
	ConditionScheduled:   true,
	ConditionRunning:     true,
	ConditionHealthy:     true,
	ConditionReady:       true,
	ConditionReconciling: true,
}
