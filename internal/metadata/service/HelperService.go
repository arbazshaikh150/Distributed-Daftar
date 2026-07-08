package service

import (
	"time"

	"github.com/arbazshaikh150/Distributed-Daftar/internal/database"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/dto"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/enums"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/model"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

/*
   I have to write a worker goroutine which polls on the metadata db
   and if some files are under replica then it will add into the queue
   a recon which will run in 5 hours or something
*/

func MarkRecovery(JobId uuid.UUID) (dto.MarkRecoveryResponse, error) {
	var response dto.MarkRecoveryResponse
	now := time.Now()
	lockExpiresAt := now.Add(2 * time.Minute)

	err := database.DB.Transaction(func(tx *gorm.DB) error {
		// Transaction
		var replica model.ReplicaData
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			First(&replica, "job_id = ?", JobId).Error; err != nil {
			return err
		}

		if replica.Status == enums.COMPLETED {
			response = dto.MarkRecoveryResponse{
				Status:  "already_completed",
				Message: "recovery job already completed",
			}
			return nil
		}

		if replica.Status == enums.PROGRESS &&
			replica.LockExpiresAt != nil &&
			replica.LockExpiresAt.After(now) {
			response = dto.MarkRecoveryResponse{
				Status:  "busy",
				Message: "recovery job is already in progress",
			}
			return nil
		}

		replica.Status = enums.PROGRESS
		replica.LockedAt = &now
		replica.LockExpiresAt = &lockExpiresAt

		if err := tx.Save(&replica).Error; err != nil {
			return err
		}

		response = dto.MarkRecoveryResponse{
			Status:        "Claimed",
			FileId:        replica.RunID,
			CopyNode:      replica.CopyNode,
			TargetNode:    replica.TargetNode,
			LockExpiresAt: lockExpiresAt,
			Message:       "recovery job claimed",
		}

		return nil
	})

	if err != nil {
		return dto.MarkRecoveryResponse{}, nil
	}

	return response, nil
}

func CompleteRecovery(JobId uuid.UUID) (dto.CompleteRecoveryResponse, error) {
	now := time.Now()

	// Doing database transaction on both table metadata as well as the replica node
	err := database.DB.Transaction(func(tx *gorm.DB) error {
		var replica model.ReplicaData
		// Locking the row
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			First(&replica, "job_id = ?", JobId).Error; err != nil {
			return err
		}

		if replica.Status == enums.COMPLETED {
			return nil
		}

		// Metadata row
		var metadata model.Metadata
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			First(&metadata, "file_id = ?", replica.RunID).Error; err != nil {
			return err
		}

		exists := (metadata.PrimaryNode == replica.TargetNode)

		for _, node := range metadata.ReplicaNodes {
			if node == replica.TargetNode {
				exists = true
				break
			}
		}

		if !exists {
			metadata.ReplicaNodes = append(metadata.ReplicaNodes, replica.TargetNode)
		}

		currentReplicaCount := 1 + len(metadata.ReplicaNodes)

		if currentReplicaCount >= metadata.ReplicationCount {
			metadata.Status = enums.COMMITTED
		}

		replica.Status = enums.COMPLETED
		replica.CompletedAt = &now
		replica.LockExpiresAt = nil
		replica.LockedAt = nil

		if err := tx.Save(&metadata).Error; err != nil {
			return err
		}

		if err := tx.Save(&replica).Error; err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return dto.CompleteRecoveryResponse{}, err
	}

	return dto.CompleteRecoveryResponse{
		Status:  "completed",
		Message: "recovery completed successfully",
	}, nil
}
