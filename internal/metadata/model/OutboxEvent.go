package model

import (
	"time"

	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/enums"
	"github.com/google/uuid"
)

type OutboxEvent struct {
	EventId     uuid.UUID          `gorm:"type:uuid;default:gen_random_uuid();primaryKey;column:event_id" json:"eventId"`
	JobId       uuid.UUID          `gorm:"type:uuid;column:job_id;not null;index" json:"jobId"`
	EventType   string             `gorm:"type:text;column:event_type;not null" json:"eventType"`
	Status      enums.StatusDetail `gorm:"type:text;column:status;not null;default:'Pending'" json:"status"`
	LockedAt    *time.Time         `gorm:"column:locked_at" json:"lockedAt,omitempty"`
	ProcessedAt *time.Time         `gorm:"column:processed_at" json:"processedAt,omitempty"`
	RetryCount  int                `gorm:"column:retry_count;not null;default:0" json:"retryCount"`
	LastError   string             `gorm:"type:text;column:last_error" json:"lastError,omitempty"`
	CreatedAt   time.Time          `gorm:"column:created_at;autoCreateTime" json:"createdAt"`
	UpdatedAt   time.Time          `gorm:"column:updated_at;autoUpdateTime" json:"updatedAt"`
}

func (OutboxEvent) TableName() string {
	return "outbox_event"
}
