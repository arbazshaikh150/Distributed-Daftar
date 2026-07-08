package model

import (
	"time"

	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/enums"
	"github.com/google/uuid"
)

type ReplicaData struct {
	JobId         uuid.UUID          `gorm:"type:uuid;default:gen_random_uuid();primaryKey;column:job_id" json:"jobId"`
	RunID         uuid.UUID          `gorm:"type:uuid;column:run_id;not null" json:"runId"`
	TargetNode    uuid.UUID          `gorm:"type:uuid;column:target_node;not null" json:"targetNode"`
	CopyNode      uuid.UUID          `gorm:"type:uuid;column:copy_node;not null" json:"copyNode"`
	Status        enums.StatusDetail `gorm:"type:text;column:status;not null;default:'Pending'" json:"status"`
	LockedAt      *time.Time         `gorm:"column:locked_at" json:"lockedAt,omitempty"`
	LockExpiresAt *time.Time         `gorm:"column:lock_expires_at" json:"lockExpiresAt,omitempty"`
	CompletedAt   *time.Time         `gorm:"column:completed_at" json:"completedAt,omitempty"`
	RetryCount    int                `gorm:"column:retry_count;not null;default:0" json:"retryCount"`
}

func (ReplicaData) TableName() string {
	return "replica_data"
}
