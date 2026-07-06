package dto

import (
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/enums"
	"github.com/google/uuid"
)

type CommitResponse struct {
	FileId            uuid.UUID          `json:"file_id"`
	ReplicationStatus enums.StatusDetail `json:"replication_status"`
	RequiredReplica   int                `json:"required_replica"`
	CurrentReplica    int                `json:"current_replica"`
	Message           string             `json:"message"`
}
