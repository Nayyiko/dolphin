package model

import (
	"time"
)

// Worker 执行器节点。调度器根据负载选择 Worker 执行任务。
type Worker struct {
	ID             string    `gorm:"primaryKey;type:varchar(64)" json:"id"`
	Address        string    `gorm:"type:varchar(255);not null" json:"address"`
	// Status: online / offline。由调度器心跳检测驱动。
	Status         string    `gorm:"type:varchar(20);not null;default:'online'" json:"status"`
	MaxConcurrency int       `gorm:"not null;default:10" json:"maxConcurrency"`
	// CurrentLoad 当前正在执行的任务数（由 Worker 心跳上报）。
	CurrentLoad    int       `gorm:"not null;default:0" json:"currentLoad"`
	LastHeartbeat  time.Time `gorm:"not null" json:"lastHeartbeat"`
	RegisteredAt   time.Time `gorm:"not null" json:"registeredAt"`
	UpdatedAt      time.Time `gorm:"not null" json:"updatedAt"`
}

// Worker 状态值。
const (
	WorkerStatusOnline  = "online"
	WorkerStatusOffline = "offline"
)
