package dto

import (
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/enums"
	"github.com/google/uuid"
)

type HeartBeatRequest struct {
	NodeId            uuid.UUID          `json:"nodeId"`
	Status            enums.StatusDetail `json:"status"`
	AvailableCapacity int64              `json:"availableCapacity"`
	TotalCapacity     int64              `json:"totalCapacity"`
}
