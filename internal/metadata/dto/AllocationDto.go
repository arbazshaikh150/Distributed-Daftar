package dto

type DataAllocationRequest struct {
	RequiredSpace       int64 `json:"requiredSpace"`
	ReplicationFactor   int   `json:"replicationFactor"`
	MinimumRequiredCopy int   `json:"minimumCopy"`
}
