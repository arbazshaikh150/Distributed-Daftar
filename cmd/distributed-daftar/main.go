package main

import (
	"fmt"
	"log"

	"github.com/arbazshaikh150/Distributed-Daftar/internal/app"
)
/*
	My metadata service can make an api call 
	to the client as well 

	i have to make a client for this as well 
	dummy client which can do the 

	hearbeat , nodestatus management and then 
	upload ( dummy in some node ) and then 
	delete from the node --> this can be done by deallocating the alllocated
	memory

	i have to make a client as well for the load testing 
*/

func main() {
	if err := app.Run(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Hello from Arbaz")
}
