package app

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/arbazshaikh150/Distributed-Daftar/internal/database"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/cache"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/controller"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/queue"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/service"
)

// Run starts the application.
func Run() error {
	// Connecting with the database
	if err := database.ConnectToDb(); err != nil {
		log.Fatal(err)
	}
	log.Println("Connected to the PostGresSQL")

	// Automigrating the tables
	if err := database.AutoMigrate(); err != nil {
		log.Println(err)
	}

	// Connecting to Redis
	if err := cache.ConnectToRedis(); err != nil {
		log.Fatal(err)
	}
	log.Println("Connected to the Redis")

	// Connecting to RabbitMq
	if err := queue.ConnetRabbitMQ(); err != nil {
		log.Fatal(err)
	}
	log.Println("Connected To RabbitMQ")

	// Starting the go routine here for recovery
	ctx := context.Background()
	go service.StartRecoveryWorker(ctx)
	log.Println("GoRoutine is being start")

	// Nodes
	mux := http.NewServeMux()
	mux.HandleFunc("POST /nodes/register", controller.NodeRegister)
	mux.HandleFunc("GET /nodes/{id}", controller.GetNodeInfo)
	mux.HandleFunc("PATCH /nodes/updatecap", controller.UpdateNodeCap)
	mux.HandleFunc("GET /nodes/active", controller.GetAllActiveNodes)

	// Heartbeat
	mux.HandleFunc("POST /nodes/heartbeat", controller.HeartBeat)

	// Metadata
	mux.HandleFunc("GET /files/{fileId}", controller.GetFile)
	mux.HandleFunc("GET /files/{fileId}/location", controller.GetFileLocation)
	mux.HandleFunc("PATCH /files/{fileId}", controller.UpdateFileVersion)
	mux.HandleFunc("POST /files/allocate", controller.AllocateNodes)
	mux.HandleFunc("POST /files/commit", controller.CommitResponse)

	// Recovery
	// Recovery
	mux.HandleFunc("POST /recover/{jobId}/mark", controller.MarkRecoveryController)
	mux.HandleFunc("POST /recover/{jobId}/complete", controller.CompleteRecoveryController)
	fmt.Println("Server is listening at port 8080")
	return http.ListenAndServe(":8080", mux)
}
