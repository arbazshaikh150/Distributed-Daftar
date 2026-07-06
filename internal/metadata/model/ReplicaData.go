package model

import "github.com/google/uuid"

type ReplicaData struct {
	JobId      uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();primaryKey;column:job_id" json:"jobId"`
	RunID      uuid.UUID `gorm:"type:uuid;column:run_id;not null" json:"runId"`
	TargetNode uuid.UUID `gorm:"type:uuid;column:target_node;not null" json:"targetNode"`
	CopyNode   uuid.UUID `gorm:"type:uuid;column:copy_node;not null" json:"copyNode"`
}

func (ReplicaData) TableName() string {
	return "replica_data"
}
