package app

import (
	"fmt"
	"log"
	"net/http"

	"github.com/arbazshaikh150/Distributed-Daftar/internal/database"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/cache"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/controller"
)

// Run starts the application.
func Run() error {
	// Connecting with the database
	if err := database.ConnectToDb(); err != nil {
		log.Fatal(err)
	}
	log.Println("Connected to the PostGresSQL")

	// Connecting to Redis
	if err := cache.ConnectToRedis(); err != nil {
		log.Fatal(err)
	}
	log.Println("Connected to the Redis")

	mux := http.NewServeMux()
	mux.HandleFunc("POST /nodes/register", controller.NodeRegister)
	mux.HandleFunc("GET /nodes/{id}", controller.GetNodeInfo)
	mux.HandleFunc("PATCH /nodes/updatecap", controller.UpdateNodeCap)
	mux.HandleFunc("GET /nodes/active", controller.GetAllActiveNodes)

	mux.HandleFunc("POST /nodes/heartbeat", controller.HeartBeat)

	fmt.Println("Server is listening at port 8080")
	return http.ListenAndServe(":8080", mux)
}
