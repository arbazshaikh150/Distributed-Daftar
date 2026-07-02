package dto

import (
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/enums"
	"github.com/google/uuid"
)

type HeartBeatResponse struct {
	NodeId  uuid.UUID          `json:"nodeId"`
	Status  enums.StatusDetail `json:"status"`
	Message string             `json:"message"`
}
