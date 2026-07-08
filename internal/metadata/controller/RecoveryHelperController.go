package controller

import (
	"encoding/json"
	"net/http"

	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/service"
	"github.com/google/uuid"
)

/*
	I should handle the idempotency property
	Request come to me --> i have to take make it register and then


	can i use the redis here ?? --> for storing the cache ??
	1st would be the request access and then storing it in the redis

	what happen when redis down --> i cannot totally dependent on redis only
	Basic purpose of redis is to store the cache data that is being used multiple
	times
	but here i am going to use it as source of truth

	is it a proper software developer method??
	No , Redis can restart can evict keys

	i have to use db only for the source of truth

	this will be done using the Status and locking
	there must be an key which will make sure that the locked is being done some x node

*/
/*
	Contract:
	if job is being already saved then i do not have to do anything
	iwill check and respond that ki job is being already saved

*/

/*
	There are multiple failure point in my code :
		1) What happen when the database service operation is done and just before sending
			the ack --> my server is down
		2) then retry will be done and then again it will add new node in the event
		3) i should have checker function in that

		Find all the points where there can be a possible issue in my code
*/

// only api would be
/*
	POST /recover/{event_id}/complete

	because if the same event come twice my node service will check the file is present in the node or not
	but for checking the node is present or not it will call metadata service (me) only
	then i have to give them the actual message

	Locking protocols should be used here also
	i don't care about who is locking the row
	the thing that matter me is that row is being locked


	status and timestamp is important

	now i have to implement two thing
	1) POST /recover/{event_id}/mark --> marking status to (PROCESSING with some expiration time)
	2) POST /recover/{event_id}/complete  --> marking it complete

	but what happens when the event copy took long time than expiration
	then what i have to do ?

	two ways db leasing should be done
	1st for marking it

	2nd for updating it
	metadata DB locks row again
	if already Completed -> return success
	else update metadata.replica_nodes + mark job Completed

	// idempotent consumer pattern , database lease pattern
*/

func MarkRecoveryController(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	jobId, err := uuid.Parse(r.PathValue("jobId"))
	if err != nil {
		http.Error(w, "invalid job id", http.StatusBadRequest)
		return
	}

	response, err := service.MarkRecovery(jobId)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func CompleteRecoveryController(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	jobId, err := uuid.Parse(r.PathValue("jobId"))
	if err != nil {
		http.Error(w, "invalid job id", http.StatusBadRequest)
		return
	}

	response, err := service.CompleteRecovery(jobId)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
