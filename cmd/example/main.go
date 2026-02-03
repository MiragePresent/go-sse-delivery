package main

import (
	"flag"
	"fmt"
	"net/http"

	"github.com/miragepresent/go-sse-delivery/messaging"
	"github.com/miragepresent/go-sse-delivery/server"
)

func main() {
	debug := flag.Bool("debug", false, "Enable debug mode with test page at /debug/")
	port := flag.String("port", ":8080", "Server port")
	flag.Parse()

	// Create channels
	ingress := make(chan *messaging.Event, 100)
	updates := make(chan *messaging.Update, 100)

	// Create handlers and processor
	handlers := server.NewHandlers(ingress, updates)
	processor := messaging.NewProcessor(ingress, updates, &messaging.EchoHandler{})

	// Start goroutines
	go handlers.Run()
	go processor.Process()

	// Setup routes
	http.HandleFunc("GET /live-updates", handlers.DeliverEndpointHandler)
	http.HandleFunc("POST /events", handlers.NewEventsHandler)

	if *debug {
		http.Handle("/debug/", http.StripPrefix("/debug/", http.FileServer(http.Dir("cmd/example/static"))))
	}

	fmt.Printf("Server starting on %s\n", *port)
	http.ListenAndServe(*port, nil)
}
