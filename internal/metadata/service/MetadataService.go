package service

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/arbazshaikh150/Distributed-Daftar/internal/database"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/dto"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/enums"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/model"
)

func GetFileData(fileId uuid.UUID) (model.Metadata, error) {
	var data model.Metadata
	err := database.DB.First(&data, "file_id = ?", fileId).Error
	if err != nil {
		return model.Metadata{}, err
	}

	return data, nil
}

func GetFileLocation(fileId uuid.UUID) (dto.LocationDTO, error) {
	data, err := GetFileData(fileId)
	if err != nil {
		return dto.LocationDTO{}, err
	}

	return dto.LocationDTO{
		Version:      data.Version,
		PrimaryNode:  data.PrimaryNode,
		ReplicaNodes: data.ReplicaNodes,
	}, nil
}

func UpdateFileVersion(fileId uuid.UUID, version int) (dto.LocationDTO, error) {
	if version <= 0 {
		return dto.LocationDTO{}, errors.New("version must be greater than zero")
	}

	var data model.Metadata
	err := database.DB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			First(&data, "file_id = ?", fileId).Error; err != nil {
			return err
		}

		data.Version = version

		if err := tx.Save(&data).Error; err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return dto.LocationDTO{}, err
	}

	return dto.LocationDTO{
		Version:      data.Version,
		PrimaryNode:  data.PrimaryNode,
		ReplicaNodes: data.ReplicaNodes,
	}, nil
}

/*
	Required files to be stored in the nodes
	1st phase passing the nodes that have the available capacity
*/
/*
	Problem : When the status is inactive then i will lost the data of the available space

	then i will return the Status is not active and the available space field will be empty

*/

func AvailableNodes(req dto.DataAllocationRequest) ([]uuid.UUID, error) {
	// I have to use redis here
	// I Have to make client as well
	// which runs on different port and does the api call / heartbeat
	reqSpace := req.RequiredSpace
	reqNodes := req.ReplicationFactor
	nodes, err := GetAvailableNodeWithCapExclude(
		reqSpace,
		reqNodes,
		map[uuid.UUID]bool{},
	)
	if err != nil {
		return nil, err
	}
	return nodes, nil

}

/*
	TODO : Anyone can call the metadata system but i am assuming it is expose to only internal apis
			i have to add the security as well and also there might be some restrictions also
			but since it is a metadata service this is what i have to expect the behaviour
*/
// Finding the Commit protocols

/*
Here Rejection means that i am not storing the data into the postgres db
and the node that are storing the data must delete it

if i am developing a full fledge system then
I saved the file in some node with some TTL --> if we are getting the commit
response from the db then permanently store it
else after ttl delete it

Metadata commit --> (failure) --> file Service --> TTL expire --> node clean up the space
by own

i do not have to worry about it
*/
func Commit(req dto.CommitRequest) (dto.CommitResponse, error) {
	// here i have to do the two phase transactional commit
	// Error handling
	if len(req.SuccessfulNodes) == 0 {
		return dto.CommitResponse{}, errors.New("No Node for being a master")
	}

	if req.TotalSuccessCount < req.MinimumReplication {
		return dto.CommitResponse{}, errors.New("minimum replication not satisfied")
	}

	primaryNode := req.SuccessfulNodes[0]
	replicaNodes := req.SuccessfulNodes[1:] // rest of the other will be the replica node

	status := enums.UNDERREPLICATED

	if req.TotalSuccessCount == req.ReplicationFactor {
		// then the status will be
		status = enums.COMMITTED
	}

	extraReplication := req.ReplicationFactor - req.TotalSuccessCount

	successfulMap := map[uuid.UUID]bool{}
	for _, node := range req.SuccessfulNodes {
		successfulMap[node] = true
	}

	targetNodes, err := GetAvailableNodeWithCapExclude(
		req.Size,
		extraReplication,
		successfulMap,
	)
	if err != nil {
		return dto.CommitResponse{}, err
	}

	if len(targetNodes) < extraReplication {
		return dto.CommitResponse{}, errors.New("not enough extra active nodes for repair")
	}

	err = database.DB.Transaction(func(tx *gorm.DB) error {
		metadata := model.Metadata{
			FileId:       req.FileId,
			Version:      req.Version,
			SizeBytes:    req.Size,
			PrimaryNode:  primaryNode,
			ReplicaNodes: replicaNodes,
			CreatedAt:    time.Now(),
			Status:       status,
		}

		if err := tx.Create(&metadata).Error; err != nil {
			return err
		}

		for _, targetNode := range targetNodes {
			replica := model.ReplicaData{
				RunID:      req.FileId,
				TargetNode: targetNode,
				CopyNode:   primaryNode,
			}

			if err := tx.Create(&replica).Error; err != nil {
				return err
			}

			// here i have to add this into the outbox_event table as well
			event := model.OutboxEvent{
				JobId:     replica.JobId,
				EventType: string(enums.ReplicaRepairRequested),
				Status:    enums.PENDING,
			}

			if err := tx.Create(&event).Error; err != nil {
				return err
			}
		}

		return nil

	})

	if err != nil {
		return dto.CommitResponse{}, err
	}

	message := "Successfully Commit the File"

	if status == enums.UNDERREPLICATED {
		message = "commit completed but file is under replicated"
	}
	return dto.CommitResponse{
		FileId:            req.FileId,
		ReplicationStatus: status,
		RequiredReplica:   req.ReplicationFactor,
		CurrentReplica:    req.TotalSuccessCount,
		Message:           message,
	}, nil
}
