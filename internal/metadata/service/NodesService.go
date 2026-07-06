package service

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/arbazshaikh150/Distributed-Daftar/internal/database"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/dto"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/enums"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/model"
)

func RegisterNode(request dto.RegisterNodeRequest) (dto.RegisterNodeResponse, error) {
	nodeId := uuid.New()

	node := model.NodesData{
		NodeId:        nodeId,
		Host:          request.Host,
		Port:          request.Port,
		TotalCapacity: request.TotalCapacity,
	}

	if err := database.DB.Create(&node).Error; err != nil {
		return dto.RegisterNodeResponse{}, err
	}

	if err := SaveNodeCache(nodeId, request.TotalCapacity); err != nil {
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

func GetNodeInfo(nodeId uuid.UUID) (dto.NodeResponse, error) {
	var node model.NodesData
	if err := database.DB.First(&node, "node_id = ?", nodeId).Error; err != nil {
		return dto.NodeResponse{}, err
	}

	return NodeResponseFromCache(node)
}

func UpdateNodeCap(id uuid.UUID, availableCapacity int64) (dto.NodeResponse, error) {
	var node model.NodesData
	if err := database.DB.First(&node, "node_id = ?", id).Error; err != nil {
		return dto.NodeResponse{}, err
	}

	if err := SaveNodeCache(id, availableCapacity); err != nil {
		return dto.NodeResponse{}, err
	}

	return NodeResponseFromCache(node)
}

func GetAllActiveNodes() ([]dto.NodeResponse, error) {
	var nodes []model.NodesData
	if err := database.DB.Find(&nodes).Error; err != nil {
		return nil, err
	}

	activeNodes := []dto.NodeResponse{}

	for _, node := range nodes {
		response, err := NodeResponseFromCache(node)
		if err != nil {
			return nil, err
		}

		if response.Status == enums.Active {
			activeNodes = append(activeNodes, response)
		}
	}

	return activeNodes, nil
}

/*
	In future i have to add the availableSize and the
	totalsize for the node

	for now , i am just taking the id and the status
*/

/*
	TODO : THERE IS SOME ERROR , WHEN I GIVE HEARTBEAT OF DEAD NODE 
			IT IS NOT UPDATING IT INSIDE THE REDIS 
			HAVE TO FIX THIS 	
*/

func HeartBeat(id uuid.UUID, availableSpace int64) (dto.HeartBeatResponse, error) {
	if err := SaveNodeCache(id, availableSpace); err != nil {
		return dto.HeartBeatResponse{}, err
	}

	return dto.HeartBeatResponse{
		NodeId:  id,
		Status:  enums.Active,
		Message: "HeartBeat Received",
	}, nil

}
