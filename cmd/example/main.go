package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/miragepresent/go-sse-delivery/core"
	t "github.com/miragepresent/go-sse-delivery/transport"
)

type EchoHandler struct{}

func (h *EchoHandler) Handle(signal *core.Signal) (*core.Update, error) {
	return &core.Update{Data: signal.Data, Mode: core.Broadcast}, nil
}

func main() {
	port := flag.String("port", ":8080", "Server port")
	flag.Parse()

	// Create channels
	ingress := make(chan *core.Signal, 100)
	updates := make(chan *core.Update, 100)

	echo := &EchoHandler{}

	// Create handlers and dispatcher
	dispatcher := core.NewDispatcher(ingress, updates,
		core.OnError(func(err error) {
			log.Printf("dispatcher error: %v\n", err)
		}),
	)
	dispatcher.Register("message", echo)
	dispatcher.Register("notification", echo)
	dispatcher.Register("alert", echo)

	signalReceiver := t.NewSignalReceiver(ingress,
		t.OnSignalReceived(func(connID string, signalType string) {
			log.Printf("signal received: connectionId=%s, type=%s\n", connID, signalType)
		}),
	)
	sseHandler := t.NewSseHandler(
		updates,
		t.OnConnected(func(connId string, active int) {
			log.Printf("new connection established %s. number of active connections %d\n", connId, active)
		}),
		t.OnDisconnected(func(connId string, active int) {
			log.Printf("lost connection %s. number of active connections %d\n", connId, active)
		}),
		t.OnError(func(err error) {
			log.Printf("SSE handler error: %v\n", err)
		}),
	)

	// Start goroutines
	go dispatcher.Start()

	// Setup routes
	http.Handle("POST /signals", signalReceiver)
	http.Handle("GET /live-updates", sseHandler)
	http.Handle("/demo/", http.StripPrefix("/demo/", http.FileServer(http.Dir("cmd/example/static"))))

	fmt.Printf("Server starting on %s\n", *port)
	http.ListenAndServe(*port, nil)
}
