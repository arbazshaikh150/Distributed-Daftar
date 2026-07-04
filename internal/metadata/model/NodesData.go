package model

import (
	"github.com/google/uuid"
)

type NodesData struct {
	NodeId        uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();primaryKey;column:node_id" json:"nodeId"`
	Host          string    `gorm:"column:host" json:"host"`
	Port          int       `gorm:"column:port" json:"port"`
	TotalCapacity int64     `gorm:"column:total_capacity" json:"totalCapacity"`
}

func (NodesData) TableName() string {
	return "nodes_data"
}
