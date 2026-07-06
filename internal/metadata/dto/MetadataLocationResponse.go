package dto

import "github.com/google/uuid"

type LocationDTO struct {
	Version      int      `json:"version"`
	PrimaryNode  uuid.UUID   `json:"primaryNode"`
	ReplicaNodes []uuid.UUID `json:"replicaNodes"`
}
