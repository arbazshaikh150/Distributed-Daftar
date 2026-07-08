package dto

import (
	"time"

	"github.com/google/uuid"
)

type MarkRecoveryResponse struct {
	Status        string    `json:"status"`
	FileId        uuid.UUID `json:"fileId,omitempty"`
	CopyNode      uuid.UUID `json:"copyNode,omitempty"`
	TargetNode    uuid.UUID `json:"targetNode,omitempty"`
	LockExpiresAt time.Time `json:"lockExpiresAt,omitempty"`
	Message       string    `json:"message"`
}

type CompleteRecoveryResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}
