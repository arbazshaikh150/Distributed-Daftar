package model

import (
	"time"

	"github.com/google/uuid"
)

type Metadata struct {
	FileId       uuid.UUID `gorm:"primaryKey;column:file_id" json:"fileId"`
	FileName     string    `gorm:"column:file_name;not null" json:"fileName"`
	Version      int       `gorm:"column:version;not null" json:"version"`
	SizeBytes    int64     `gorm:"column:size_bytes;not null" json:"sizeBytes"`
	PrimaryNode  string    `gorm:"column:primary_node;not null" json:"primaryNode"`
	ReplicaNodes []string  `gorm:"type:jsonb;serializer:json;column:replica_nodes" json:"replicaNodes"`
	CreatedAt    time.Time `gorm:"column:created_at;not null" json:"createdAt"`
}

func (Metadata) TableName() string {
	return "metadata"
}
