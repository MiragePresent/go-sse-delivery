package main

import (
	"flag"
	"fmt"
	"net/http"

	"github.com/miragepresent/go-sse-delivery/core"
	"github.com/miragepresent/go-sse-delivery/transport"
)

func main() {
	port := flag.String("port", ":8080", "Server port")
	flag.Parse()

	// Create channels
	ingress := make(chan *core.Signal, 100)
	updates := make(chan *core.Update, 100)

	// Create handlers and dispatcher
	dispatcher := core.NewDispatcher(ingress, updates)
	dispatcher.Register("message", &core.EchoHandler{})
	dispatcher.Register("notification", &core.EchoHandler{})
	dispatcher.Register("alert", &core.EchoHandler{})

	signalReceiver := transport.NewSignalReceiver(ingress)
	sseHandler := transport.NewSseHandler(updates)

	// Start goroutines
	go dispatcher.Run()

	// Setup routes
	http.Handle("POST /signals", signalReceiver)
	http.Handle("GET /live-updates", sseHandler)
	http.Handle("/demo/", http.StripPrefix("/demo/", http.FileServer(http.Dir("cmd/example/static"))))

	fmt.Printf("Server starting on %s\n", *port)
	http.ListenAndServe(*port, nil)
}
