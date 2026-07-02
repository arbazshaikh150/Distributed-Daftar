package service

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/arbazshaikh150/Distributed-Daftar/internal/database"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/cache"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/dto"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/enums"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/model"
)

func RegisterNode(request dto.RegisterNodeRequest) (dto.RegisterNodeResponse, error) {
	nodeId := uuid.New()

	node := model.NodesData{
		NodeId:            nodeId,
		Host:              request.Host,
		Port:              request.Port,
		Status:            string(enums.Active),
		TotalCapacity:     request.TotalCapacity,
		AvailableCapacity: request.TotalCapacity,
	}

	if err := database.DB.Create(&node).Error; err != nil {
		return dto.RegisterNodeResponse{}, err
	}

	fmt.Println("Saved the node in the database")

	response := dto.RegisterNodeResponse{
		NodeID:  nodeId,
		Status:  enums.Active,
		Message: "Node Register Successfully",
	}

	return response, nil
}

func GetNodeInfo(nodeId uuid.UUID) (model.NodesData, error) {
	var node model.NodesData
	if err := database.DB.First(&node, "node_id = ?", nodeId).Error; err != nil {
		return model.NodesData{}, err
	}

	return node, nil
}

func UpdateNodeCap(id uuid.UUID, availableCapacity int64) (model.NodesData, error) {
	var node model.NodesData
	err := database.DB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			First(&node, "node_id = ?", id).Error; err != nil {
			return err
		}

		node.AvailableCapacity = availableCapacity

		if err := tx.Save(&node).Error; err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return model.NodesData{}, err
	}

	return node, nil
}

func GetAllActiveNodes() ([]model.NodesData, error) {
	var allActiveNode []model.NodesData
	err := database.DB.
		Where("status = ?", string(enums.Active)).
		Find(&allActiveNode).Error

	if err != nil {
		return nil, err
	}

	return allActiveNode, nil
}

/*
	In future i have to add the availableSize and the
	totalsize for the node

	for now , i am just taking the id and the status
*/

func HeartBeat(id uuid.UUID) (dto.HeartBeatResponse, error) {
	key := "node:" + id.String() + ":heartbeat"

	retryCount, err := strconv.Atoi(os.Getenv("RETRY_COUNT"))
	if err != nil {
		retryCount = 3
	}

	heartbeat, err := strconv.Atoi(os.Getenv("HEARTBEAT_INTERVAL"))
	if err != nil {
		heartbeat = 5
	}

	ttl := time.Duration(retryCount*heartbeat) * time.Second

	context := context.Background()
	err = cache.RedisClient.Set(context, key, string(enums.Active), ttl).Err()
	if err != nil {
		return dto.HeartBeatResponse{}, err
	}

	return dto.HeartBeatResponse{
		NodeId:  id,
		Status:  enums.Active,
		Message: "HeartBeat Received",
	}, nil

}
