package manager

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/yourname/dolphin/internal/pkg/model"
)

// ErrNotFound 任务不存在。
var ErrNotFound = errors.New("task not found")

// ErrInvalidCron cron 表达式无效。
var ErrInvalidCron = errors.New("invalid cron expression")

// Manager 任务管理器。负责 Task CRUD 和 MySQL 持久化。
type Manager struct {
	db *gorm.DB
}

// NewManager 创建任务管理器。
func NewManager(db *gorm.DB) *Manager {
	return &Manager{db: db}
}

// DB 返回底层 gorm.DB（供服务层直接查询）。
func (m *Manager) DB() *gorm.DB {
	return m.db
}

// AutoMigrate 自动建表。
func (m *Manager) AutoMigrate() error {
	return m.db.AutoMigrate(
		&model.Task{},
		&model.TaskLog{},
		&model.Worker{},
		&model.TaskCondition{},
	)
}

// Create 创建任务。
func (m *Manager) Create(ctx context.Context, task *model.Task) (*model.Task, error) {
	task.ID = uuid.NewString()
	task.Status = model.TaskStatusActive
	if task.Timeout == 0 {
		task.Timeout = 30
	}
	if task.MaxRetries == 0 {
		task.MaxRetries = 3
	}
	if task.HandlerType == "" {
		task.HandlerType = model.HandlerTypeHTTP
	}
	now := time.Now()
	task.CreatedAt = now
	task.UpdatedAt = now

	if err := m.db.WithContext(ctx).Create(task).Error; err != nil {
		return nil, fmt.Errorf("create task: %w", err)
	}
	return task, nil
}

// Update 更新任务（乐观锁：基于 UpdatedAt）。
func (m *Manager) Update(ctx context.Context, task *model.Task) (*model.Task, error) {
	var existing model.Task
	if err := m.db.WithContext(ctx).First(&existing, "id = ?", task.ID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get task: %w", err)
	}

	task.UpdatedAt = time.Now()
	result := m.db.WithContext(ctx).Model(&existing).Updates(map[string]any{
		"name":         task.Name,
		"cron_expr":    task.CronExpr,
		"handler":      task.Handler,
		"handler_type": task.HandlerType,
		"params":       task.Params,
		"timeout":      task.Timeout,
		"max_retries":  task.MaxRetries,
		"depend_on":    task.DependOn,
		"dep_policy":   task.DepPolicy,
		"updated_at":   task.UpdatedAt,
	})
	if result.Error != nil {
		return nil, fmt.Errorf("update task: %w", result.Error)
	}
	existing.UpdatedAt = task.UpdatedAt
	return &existing, nil
}

// SetStatus 修改任务状态（active/paused）。
func (m *Manager) SetStatus(ctx context.Context, id, status string) (*model.Task, error) {
	if status != model.TaskStatusActive && status != model.TaskStatusPaused {
		return nil, fmt.Errorf("invalid status %q", status)
	}
	var existing model.Task
	if err := m.db.WithContext(ctx).First(&existing, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if err := m.db.WithContext(ctx).Model(&existing).
		Updates(map[string]any{"status": status, "updated_at": time.Now()}).Error; err != nil {
		return nil, err
	}
	existing.Status = status
	return &existing, nil
}

// Delete 软删除：设置 DeletionTimestamp，由 Reconciler 优雅清理（Finalizer 模式）。
func (m *Manager) Delete(ctx context.Context, id string) error {
	var existing model.Task
	if err := m.db.WithContext(ctx).First(&existing, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrNotFound
		}
		return err
	}
	now := time.Now()
	return m.db.WithContext(ctx).Model(&existing).
		Updates(map[string]any{
			"deletion_timestamp": &now,
			"status":             model.TaskStatusDeleted,
			"updated_at":         now,
		}).Error
}

// Get 获取单个任务。
func (m *Manager) Get(ctx context.Context, id string) (*model.Task, error) {
	var t model.Task
	if err := m.db.WithContext(ctx).First(&t, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &t, nil
}

// List 列出任务，支持按状态过滤和分页。
func (m *Manager) List(ctx context.Context, status string, offset, limit int) ([]model.Task, int64, error) {
	q := m.db.WithContext(ctx).Model(&model.Task{})
	if status != "" {
		q = q.Where("status = ?", status)
	}
	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	if limit <= 0 {
		limit = 20
	}
	var tasks []model.Task
	if err := q.Order("created_at DESC").Offset(offset).Limit(limit).Find(&tasks).Error; err != nil {
		return nil, 0, err
	}
	return tasks, total, nil
}

// ListDueTasks 列出所有到期待调度的任务。
// 条件: status=active 且 next_run_at <= now 且未标记删除。
func (m *Manager) ListDueTasks(ctx context.Context, now time.Time) ([]model.Task, error) {
	var tasks []model.Task
	err := m.db.WithContext(ctx).
		Where("status = ? AND next_run_at <= ? AND deletion_timestamp IS NULL", model.TaskStatusActive, now).
		Find(&tasks).Error
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

// ListAll 列出全部未删除任务。用于 DAG 环检测时加载全量依赖图。
func (m *Manager) ListAll(ctx context.Context) ([]model.Task, error) {
	var tasks []model.Task
	if err := m.db.WithContext(ctx).
		Where("deletion_timestamp IS NULL").
		Find(&tasks).Error; err != nil {
		return nil, err
	}
	return tasks, nil
}

// UpdateNextRunAt 更新任务的下一次触发时间。
func (m *Manager) UpdateNextRunAt(ctx context.Context, id string, nextRunAt time.Time) error {
	return m.db.WithContext(ctx).Model(&model.Task{}).
		Where("id = ?", id).
		Update("next_run_at", nextRunAt).Error
}
