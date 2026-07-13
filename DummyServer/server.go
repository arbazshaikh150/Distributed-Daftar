package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/dto"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type DummyNode struct {
	LocalID           uuid.UUID   `json:"localId"`
	NodeID            uuid.UUID   `json:"nodeId"`
	Host              string      `json:"host"`
	Port              int         `json:"port"`
	TotalCapacity     int64       `json:"totalCapacity"`
	AvailableCapacity int64       `json:"availableCapacity"`
	Files             []uuid.UUID `json:"files"`
}

type CreateNodeRequest struct {
	Host              string `json:"host"`
	Port              int    `json:"port"`
	TotalCapacity     int64  `json:"totalCapacity"`
	AvailableCapacity int64  `json:"availableCapacity"`
}

type FileSubmitRequest struct {
	FileSpace         int64 `json:"fileSpace"`
	MinimumReplica    int   `json:"minimumReplica"`
	ReplicationFactor int   `json:"replicationFactor"`
}

type RepairMessage struct {
	EventID    uuid.UUID `json:"eventId"`
	JobID      uuid.UUID `json:"jobId"`
	FileID     uuid.UUID `json:"fileId"`
	CopyNode   uuid.UUID `json:"copyNode"`
	TargetNode uuid.UUID `json:"targetNode"`
}

type AllocationResponse struct {
	FileID uuid.UUID   `json:"fileId"`
	Nodes  []uuid.UUID `json:"nodes"`
}

type Server struct {
	metadataURL string
	httpClient  *http.Client

	mu    sync.RWMutex
	nodes map[uuid.UUID]*DummyNode
}

func NewServer(metadataURL string) *Server {
	return &Server{
		metadataURL: strings.TrimRight(metadataURL, "/"),
		httpClient:  &http.Client{Timeout: 10 * time.Second},
		nodes:       make(map[uuid.UUID]*DummyNode),
	}
}

func (s *Server) CreateNodeController(w http.ResponseWriter, r *http.Request) {
	var req CreateNodeRequest
	fmt.Println("Controller is being called")
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Host == "" {
		req.Host = "localhost"
	}
	if req.TotalCapacity <= 0 {
		http.Error(w, "totalCapacity must be greater than zero", http.StatusBadRequest)
		return
	}
	if req.AvailableCapacity == 0 {
		req.AvailableCapacity = req.TotalCapacity
	}

	node := &DummyNode{
		Host:              req.Host,
		Port:              req.Port,
		TotalCapacity:     req.TotalCapacity,
		AvailableCapacity: req.AvailableCapacity,
		Files:             make([]uuid.UUID, 0),
	}

	nodeID, err := s.registerToMetadata(node)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	node.NodeID = nodeID

	s.mu.Lock()
	s.nodes[node.NodeID] = node
	s.mu.Unlock()

	go s.startHeartbeat(context.Background(), node.NodeID)

	writeJSON(w, http.StatusCreated, node)
}

func (s *Server) ListNodeController(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nodes := make([]*DummyNode, 0, len(s.nodes))
	for _, node := range s.nodes {
		nodes = append(nodes, node)
	}

	writeJSON(w, http.StatusOK, nodes)
}

func (s *Server) FileSubmitController(w http.ResponseWriter, r *http.Request) {
	var req FileSubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if err := s.HandleFileSubmit(req.FileSpace, req.MinimumReplica, req.ReplicationFactor); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"message": "file submitted successfully",
	})
}

func (s *Server) registerToMetadata(node *DummyNode) (uuid.UUID, error) {
	payload := dto.RegisterNodeRequest{
		Host:          node.Host,
		Port:          node.Port,
		TotalCapacity: node.TotalCapacity,
	}

	var response dto.RegisterNodeResponse
	fmt.Print("I am in register to metadata function")

	if err := s.postJSON("/nodes/register", payload, &response); err != nil {
		return uuid.Nil, fmt.Errorf("metadata register failed: %w", err)
	}
	if response.NodeID == uuid.Nil {
		return uuid.Nil, errors.New("metadata register did not return nodeId")
	}
	node.LocalID = response.NodeID
	return response.NodeID, nil
}

func (s *Server) startHeartbeat(ctx context.Context, nodeID uuid.UUID) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			node, ok := s.findNode(nodeID)
			if !ok {
				return
			}

			payload := dto.HeartBeatRequest{
				NodeId:            node.NodeID,
				AvailableCapacity: node.AvailableCapacity,
				TotalCapacity:     node.TotalCapacity,
			}

			if err := s.postJSON("/nodes/heartbeat", payload, nil); err != nil {
				log.Printf("heartbeat failed for node %s: %v", node.NodeID, err)
			}
		}
	}
}

func (s *Server) StartRepairConsumer(ctx context.Context) error {
	const consumerWorkerCount = 5

	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://guest:guest@localhost:5672/"
	}

	queueName := os.Getenv("REPLICA_REPAIR_QUEUE")
	if queueName == "" {
		queueName = "replica-repair"
	}

	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		return err
	}

	setupChannel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return err
	}

	_, err = setupChannel.QueueDeclare(queueName, true, false, false, false, nil)
	_ = setupChannel.Close()
	if err != nil {
		conn.Close()
		return err
	}

	for workerID := 0; workerID < consumerWorkerCount; workerID++ {
		if err := s.StartRepairConsumerWorker(ctx, conn, queueName, workerID); err != nil {
			conn.Close()
			return err
		}
	}

	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	return nil
}

func (s *Server) StartRepairConsumerWorker(ctx context.Context, conn *amqp.Connection, queueName string, workerID int) error {
	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	if err := channel.Qos(1, 0, false); err != nil {
		channel.Close()
		return err
	}
	consumerName := fmt.Sprintf(
		"dummy-server-worker-%d",
		workerID,
	)

	messages, err := channel.Consume(
		queueName,
		consumerName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		channel.Close()
		return err
	}

	go func() {
		defer channel.Close()

		for {
			select {
			case <-ctx.Done():
				return

			case message, ok := <-messages:
				if !ok {
					return
				}

				// Process, then Ack/Nack.
				if err := s.handleRepairMessage(message.Body); err != nil {
					log.Printf("repair event failed in worker %d: %v", workerID, err)
					if nackErr := message.Nack(false, false); nackErr != nil {
						log.Printf("failed to nack message in worker %d: %v", workerID, nackErr)
					}
					continue
				}

				if err := message.Ack(false); err != nil {
					log.Printf("failed to ack message in worker %d: %v", workerID, err)
				}
			}
		}
	}()

	return nil
}

func (s *Server) handleRepairMessage(body []byte) error {
	var event RepairMessage
	if err := json.Unmarshal(body, &event); err != nil {
		return err
	}

	var markResponse dto.MarkRecoveryResponse
	if err := s.postJSON(fmt.Sprintf("/recover/%s/mark", event.JobID), struct{}{}, &markResponse); err != nil {
		return fmt.Errorf("mark recovery failed: %w", err)
	}

	status := strings.ToLower(markResponse.Status)
	switch status {
	case "already_completed":
		return nil
	case "busy":
		log.Printf("recovery job %s is already being processed; acknowledging duplicate message", event.JobID)
		return nil
	case "claimed":
	default:
		return fmt.Errorf("unexpected recovery mark status %q for job %s", markResponse.Status, event.JobID)
	}

	if err := s.copyFile(event.CopyNode, event.TargetNode, event.FileID); err != nil {
		return err
	}

	var completeResponse dto.CompleteRecoveryResponse
	if err := s.postJSON(fmt.Sprintf("/recover/%s/complete", event.JobID), struct{}{}, &completeResponse); err != nil {
		return fmt.Errorf("complete recovery failed: %w", err)
	}

	log.Printf("recovery completed for job=%s file=%s status=%s", event.JobID, event.FileID, completeResponse.Status)
	return nil
}

func (s *Server) copyFile(copyNodeID uuid.UUID, targetNodeID uuid.UUID, fileID uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, sourceExists := s.nodes[copyNodeID]
	if !sourceExists {
		return fmt.Errorf("copy node %s is not present in dummy memory", copyNodeID)
	}

	target := s.nodes[targetNodeID]
	if target == nil {
		return fmt.Errorf("target node %s is not present in dummy memory", targetNodeID)
	}

	for _, existingFile := range target.Files {
		if existingFile == fileID {
			return nil
		}
	}

	target.Files = append(target.Files, fileID)
	return nil
}

func (s *Server) findNode(nodeID uuid.UUID) (*DummyNode, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	node, ok := s.nodes[nodeID]
	if !ok {
		return nil, false
	}

	copyNode := *node
	return &copyNode, true
}

func (s *Server) postJSON(path string, payload any, response any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	request, err := http.NewRequest(http.MethodPost, s.metadataURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")

	httpResponse, err := s.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer httpResponse.Body.Close()

	if httpResponse.StatusCode < 200 || httpResponse.StatusCode >= 300 {
		return fmt.Errorf("metadata returned status %s", httpResponse.Status)
	}

	if response == nil {
		return nil
	}

	return json.NewDecoder(httpResponse.Body).Decode(response)
}

func (s *Server) HandleFileSubmit(fileSpace int64, minimumReplica int, replicationFactor int) error {
	if fileSpace <= 0 {
		return errors.New("fileSpace must be greater than zero")
	}
	if minimumReplica <= 0 {
		return errors.New("minimumReplica must be greater than zero")
	}
	if replicationFactor < minimumReplica {
		return errors.New("replicationFactor must be greater than or equal to minimumReplica")
	}

	var allocation AllocationResponse
	allocateRequest := dto.DataAllocationRequest{
		RequiredSpace:       fileSpace,
		ReplicationFactor:   replicationFactor,
		MinimumRequiredCopy: minimumReplica,
	}

	if err := s.postJSON("/files/allocate", allocateRequest, &allocation); err != nil {
		return fmt.Errorf("file allocation failed: %w", err)
	}
	if allocation.FileID == uuid.Nil {
		return errors.New("file allocation did not return fileId")
	}
	if len(allocation.Nodes) < minimumReplica {
		return fmt.Errorf("metadata returned %d nodes, minimum required is %d", len(allocation.Nodes), minimumReplica)
	}

	nodes := append([]uuid.UUID(nil), allocation.Nodes...)
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})

	successCount := minimumReplica
	if len(nodes) > minimumReplica {
		successCount += rand.Intn(len(nodes) - minimumReplica + 1)
	}

	successfulNodes := nodes[:successCount]
	failedNodes := nodes[successCount:]

	s.storeFileOnSuccessfulNodes(allocation.FileID, fileSpace, successfulNodes)

	commitRequest := dto.CommitRequest{
		FileId:             allocation.FileID,
		Version:            1,
		ReplicationFactor:  replicationFactor,
		MinimumReplication: minimumReplica,
		SuccessfulNodes:    successfulNodes,
		FailedNodes:        failedNodes,
		TotalSuccessCount:  len(successfulNodes),
		Size:               fileSpace,
	}

	if err := s.postJSON("/files/commit", commitRequest, nil); err != nil {
		return fmt.Errorf("file commit failed: %w", err)
	}

	log.Printf(
		"file submit completed file=%s success=%d failed=%d",
		allocation.FileID,
		len(successfulNodes),
		len(failedNodes),
	)
	return nil
}

func (s *Server) storeFileOnSuccessfulNodes(fileID uuid.UUID, fileSpace int64, successfulNodes []uuid.UUID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, nodeID := range successfulNodes {
		node := s.nodes[nodeID]
		if node == nil {
			continue
		}

		exists := false
		for _, existingFile := range node.Files {
			if existingFile == fileID {
				exists = true
				break
			}
		}
		if !exists {
			node.Files = append(node.Files, fileID)
		}

		if node.AvailableCapacity >= fileSpace {
			node.AvailableCapacity -= fileSpace
		} else {
			node.AvailableCapacity = 0
		}
	}
}

func writeJSON(w http.ResponseWriter, statusCode int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(value)
}

func main() {
	metadataURL := os.Getenv("METADATA_URL")
	if metadataURL == "" {
		metadataURL = "http://localhost:8080"
	}

	listenAddr := os.Getenv("DUMMY_SERVER_ADDR")
	if listenAddr == "" {
		listenAddr = ":9001"
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := NewServer(metadataURL)
	if err := server.StartRepairConsumer(ctx); err != nil {
		log.Printf("rabbitmq consumer is not running: %v", err)
	} else {
		log.Println("rabbitmq consumer started")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /dummy/nodes", server.CreateNodeController)
	mux.HandleFunc("GET /dummy/nodes", server.ListNodeController)
	mux.HandleFunc("POST /dummy/files/upload", server.FileSubmitController)

	log.Printf("dummy server listening on %s", listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, mux))
}
