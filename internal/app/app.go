package app

import (
	"net/http"
	"fmt"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/controller"
)

// Run starts the application.
func Run() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/nodes/register" , controller.NodeRegister)
	fmt.Println("Server is listening at port 8080")
	return http.ListenAndServe(":8080" , mux)
}
