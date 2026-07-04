package controller

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/dto"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/service"
)

func UpdateFileVersion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPatch {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	fileId, err := uuid.Parse(r.PathValue("fileId"))
	if err != nil {
		http.Error(w, "Invalid file id", http.StatusBadRequest)
		return
	}

	type ReqDTO struct {
		Version string `json:"version"`
	}

	var request ReqDTO
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	version, err := strconv.Atoi(request.Version)
	if err != nil || version <= 0 {
		http.Error(w, "Invalid version", http.StatusBadRequest)
		return
	}

	response, err := service.UpdateFileVersion(fileId, version)
	if err != nil {
		http.Error(w, "Failed to update file version", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func GetFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	fileId, err := uuid.Parse(r.PathValue("fileId"))
	if err != nil {
		http.Error(w, "Invalid file id", http.StatusBadRequest)
		return
	}

	response, err := service.GetFileData(fileId)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			http.Error(w, "File not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Error fetching the file information", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func GetFileLocation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	fileId, err := uuid.Parse(r.PathValue("fileId"))
	if err != nil {
		http.Error(w, "Invalid file id", http.StatusBadRequest)
		return
	}

	response, err := service.GetFileLocation(fileId)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			http.Error(w, "File not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Error fetching file location", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// Main Battle begin here
/*
	Updating the files :
	Storing the metadata --> Pointing the nodes that are available
	locking the nodes
	making the data consistent and also updating the result without having data
	inconsistency
*/
// I have to implement the two phase commit protocol
/*
	First i will allocate the nodes and then after that i will be getting the nodes
	that actually stored in the nodes and minimum replications
*/

// 1ST PHASE --> REQUEST
/*
	This is partially correct 
	i have to store the metadata also 
	for proper management
*/
func AllocateNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	var req dto.DataAllocationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid Parameters in request body", http.StatusBadRequest)
		return
	}

	response, err := service.AvailableNodes(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fileId := uuid.New()
	/*
		Some db operation should be done here
		for storing the logs
	*/

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]any{
		"fileId" : fileId,
		"nodes": response,
	})
}


/*
	2md phase 
	file service will do the save operation and after that it will make 
	a commit request 
	which contains the 
*/
