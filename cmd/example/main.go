package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/miragepresent/go-sse-delivery/core"
	"github.com/miragepresent/go-sse-delivery/storage"
	t "github.com/miragepresent/go-sse-delivery/transport"
	"github.com/redis/go-redis/v9"
)

type EchoHandler struct{}

func (h *EchoHandler) Handle(signal *core.Signal) (*core.Update, error) {
	return &core.Update{Data: signal.Data, Mode: core.Broadcast}, nil
}

func main() {
	port := flag.String("port", ":8080", "Server port")
	redisAddr := flag.String("redis", "", "Redis address (e.g., localhost:6379). If empty, uses in-memory channels")
	historyCount := flag.Int("history", 10, "Number of historical updates to send on new connection (Redis only)")
	flag.Parse()

	// Check for Redis address from environment variable
	if *redisAddr == "" {
		*redisAddr = os.Getenv("REDIS_ADDR")
	}

	var signals storage.Storage
	var updates storage.Storage

	if *redisAddr != "" {
		log.Printf("Using Redis storage at %s\n", *redisAddr)
		client := redis.NewClient(&redis.Options{
			Addr: *redisAddr,
		})

		// Test connection
		ctx := context.Background()
		if err := client.Ping(ctx).Err(); err != nil {
			log.Fatalf("Failed to connect to Redis: %v", err)
		}

		signals = storage.NewRedisStorage(client, "sse:signals",
			storage.WithConsumerGroup("dispatchers"),
			storage.WithMaxLen(10000),
			storage.WithDecoder(func(id string, data []byte) (storage.Message, error) {
				return core.DecodeSignal(id, data)
			}),
		)

		updates = storage.NewRedisStorage(client, "sse:updates",
			storage.WithMaxLen(1000),
			storage.WithDecoder(func(id string, data []byte) (storage.Message, error) {
				return core.DecodeUpdate(id, data)
			}),
		)
	} else {
		log.Println("Using in-memory channel storage")
		signals = storage.NewChannelStorage()
		updates = storage.NewChannelStorage()
		*historyCount = 0 // No history for channel storage
	}

	echo := &EchoHandler{}

	// Create dispatcher
	dispatcher := core.NewDispatcher(signals, updates,
		core.OnError(func(err error) {
			log.Printf("dispatcher error: %v\n", err)
		}),
	)
	dispatcher.Register("message", echo)
	dispatcher.Register("notification", echo)
	dispatcher.Register("alert", echo)

	// Create HTTP handlers
	signalReceiver := t.NewSignalReceiver(signals,
		t.OnSignalReceived(func(connID string, signalType string) {
			log.Printf("signal received: connectionId=%s, type=%s\n", connID, signalType)
		}),
	)

	sseHandler := t.NewSseHandler(updates,
		t.WithHistoryCount(*historyCount),
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

	// Start dispatcher
	ctx := context.Background()
	go func() {
		if err := dispatcher.Start(ctx); err != nil {
			log.Printf("dispatcher stopped: %v\n", err)
		}
	}()

	// Setup routes
	http.Handle("POST /signals", signalReceiver)
	http.Handle("GET /live-updates", sseHandler)
	http.Handle("/demo/", http.StripPrefix("/demo/", http.FileServer(http.Dir("cmd/example/static"))))

	fmt.Printf("Server starting on %s\n", *port)
	http.ListenAndServe(*port, nil)
}
