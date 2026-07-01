package dto

import (
	"github.com/google/uuid"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/enums"
)

type RegisterNodeResponse struct {
	NodeID  uuid.UUID          `json:"nodeId"`
	Status  enums.StatusDetail `json:"status"`
	Message string             `json:"message"`
}
