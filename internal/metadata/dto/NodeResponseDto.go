package dto

import (
	"github.com/google/uuid"

	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/enums"
)

type NodeResponse struct {
	NodeId            uuid.UUID          `json:"nodeId"`
	Host              string             `json:"host"`
	Port              int                `json:"port"`
	Status            enums.StatusDetail `json:"status"`
	TotalCapacity     int64              `json:"totalCapacity"`
	AvailableCapacity int64              `json:"availableCapacity"`
}
