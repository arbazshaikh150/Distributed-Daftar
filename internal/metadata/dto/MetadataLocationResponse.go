package dto

type LocationDTO struct {
	Version      int      `json:"version"`
	PrimaryNode  string   `json:"primaryNode"`
	ReplicaNodes []string `json:"replicaNodes"`
}
