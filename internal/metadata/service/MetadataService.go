package service

import (
	"errors"
	"math/rand/v2"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/arbazshaikh150/Distributed-Daftar/internal/database"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/dto"
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

func AvailableNodes(req dto.DataAllocationRequest) ([]string, error) {
	// I have to use redis here
	// I Have to make client as well
	// which runs on different port and does the api call / heartbeat
	reqSpace := req.RequiredSpace
	reqNodes := req.ReplicationFactor
	if reqSpace <= 0 {
		return nil, errors.New("required space must be greater than zero")
	}

	if reqNodes <= 0 {
		return nil, errors.New("replication factor must be greater than zero")
	}

	// Fetch from the redis and the return the nodes
	keys, err := RedisKeys("node:*")
	if err != nil {
		return nil, err
	}

	// Iterating on all the nodes and taking the random reqNodes
	availableNodes := []string{}

	for _, key := range keys {
		data, err := RedisHashGetAll(key)
		if err != nil {
			return nil, err
		}

		availableSpace, err := strconv.ParseInt(data["availableSpace"], 10, 64)
		if err != nil {
			continue
		}

		if availableSpace >= reqSpace {
			nodeId := strings.TrimPrefix(key, "node:")
			availableNodes = append(availableNodes, nodeId)
		}
	}

	if len(availableNodes) < reqNodes {
		return nil, errors.New("Not enough available nodes")
	}
	rand.Shuffle(len(availableNodes), func(i, j int) {
		availableNodes[i], availableNodes[j] = availableNodes[j], availableNodes[i]
	})

	return availableNodes[:reqNodes], nil

}
