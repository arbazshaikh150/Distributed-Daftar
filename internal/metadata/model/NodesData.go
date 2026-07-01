package model

import (
	"github.com/google/uuid"
)

type NodesData struct {
	NodeID            uuid.UUID `gorm:"type:uuid;default:gen_random_uuid();primaryKey;column:node_id"`
	Host              string    `gorm:"column:host"`
	Port              int       `gorm:"column:port"`
	Status            string    `gorm:"column:status"`
	TotalCapacity     int64     `gorm:"column:total_capacity"`
	AvailableCapacity int64     `gorm:"column:available_capacity"`
}

func (NodesData) TableName() string {
	return "nodes_data"
}
