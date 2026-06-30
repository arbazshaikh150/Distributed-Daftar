package main

import (
	"fmt"
	"log"

	"github.com/arbazshaikh150/Distributed-Daftar/internal/app"
)

func main() {
	if err := app.Run(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Distributed Daftar started")
}
