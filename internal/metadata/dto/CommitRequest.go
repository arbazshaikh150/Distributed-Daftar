package dto

import "github.com/google/uuid"

type CommitRequest struct {
	FileId             uuid.UUID   `json:"file_id"`
	Version            int         `json:"version"`
	ReplicationFactor  int         `json:"replication_factor"`
	MinimumReplication int         `json:"minimum_replication"`
	SuccessfulNodes    []uuid.UUID `json:"successful_nodes"`
	FailedNodes        []uuid.UUID `json:"failed_nodes"`
	TotalSuccessCount  int         `json:"total_success_count"`
	Size               int64       `json:"size"`
}
