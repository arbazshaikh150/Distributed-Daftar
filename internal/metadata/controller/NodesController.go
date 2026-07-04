package controller

import (
	"encoding/json"
	"net/http"

	"github.com/google/uuid"

	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/dto"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/service"
)

/*
	TODO : I Have to add the status with the help of redis and then do the thing
			via an api call to redis 
*/

func NodeRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request dto.RegisterNodeRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	/*
		Request Should also be validated
	*/
	// Saving inside the database
	response, err := service.RegisterNode(request)
	if err != nil {
		http.Error(w, "Failed to register Node", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	json.NewEncoder(w).Encode(response)

}

func GetNodeInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := r.PathValue("id")
	nodeId, err := uuid.Parse(id)

	if err != nil {
		http.Error(w, "Invalid Node Id", http.StatusBadRequest)
		return
	}
	// getting node_id from the request params
	// Querying based on the node id
	node, err := service.GetNodeInfo(nodeId)
	if err != nil {
		http.Error(w, "Invalid Node Id", http.StatusNotFound)
		return
	}

	// Sending the response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(node)
}

// Updating the nodeCapacity Functions
func UpdateNodeCap(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPatch {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	type ReqDTO struct {
		Id                uuid.UUID `json:"id"`
		AvailableCapacity int64     `json:"availableCapacity"`
	}
	// Updating the database
	// Using the locking and updating from the database
	var request ReqDTO
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, "Failed to Parse the request", http.StatusBadRequest)
		return
	}

	// Using transactions here
	node, err := service.UpdateNodeCap(request.Id, request.AvailableCapacity)
	if err != nil {
		http.Error(w, "Failed to update available capacity", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(node)

}

// All active Nodes
func GetAllActiveNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Fetching from the database and then sending the response for all node
	allActiveNode, err := service.GetAllActiveNodes()
	if err != nil {
		http.Error(w, "Error occured while fetching from the db ", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]any{
		"activeNodes": allActiveNode,
	})

}

func HeartBeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var request dto.HeartBeatRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, "Invalid Node Id Request", http.StatusBadRequest)
		return
	}
	response, err := service.HeartBeat(request.NodeId)
	if err != nil {
		http.Error(w, "Failed to process heartbeat", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}
