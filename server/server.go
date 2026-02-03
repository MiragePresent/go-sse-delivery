package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/miragepresent/go-sse-delivery/messaging"
)

type Config struct {
	Port     string
	SSEPath  string
	PostPath string
	Debug    bool
}

func DefaultConfig() Config {
	return Config{
		Port:     ":8080",
		SSEPath:  "/live-updates",
		PostPath: "/events",
	}
}

type sseClient struct {
	id   string
	send chan string
}

type Server struct {
	config     Config
	storage    messaging.EventsStorage
	queue      messaging.UpdatesQueue
	processor  *messaging.Processor
	clients    map[*sseClient]bool
	register   chan *sseClient
	unregister chan *sseClient
	mu         sync.RWMutex
}

func NewServer(config Config) *Server {
	storage := messaging.NewChannelStorage(100)
	queue := messaging.NewChannelQueue(100)
	processor := messaging.NewProcessor(storage, queue, &messaging.EchoHandler{})

	return &Server{
		config:     config,
		storage:    storage,
		queue:      queue,
		processor:  processor,
		clients:    make(map[*sseClient]bool),
		register:   make(chan *sseClient),
		unregister: make(chan *sseClient),
	}
}

func (s *Server) Start() {
	go s.run()
	go s.processor.Run()
	go s.broadcastUpdates()

	http.HandleFunc("GET "+s.config.SSEPath, s.sseHandler)
	http.HandleFunc("POST "+s.config.PostPath, s.postEventHandler)
	if s.config.Debug {
		http.Handle("/debug/", http.StripPrefix("/debug/", http.FileServer(http.Dir("static"))))
	}

	fmt.Printf("Server starting on %s\n", s.config.Port)
	http.ListenAndServe(s.config.Port, nil)
}

func (s *Server) run() {
	for {
		select {
		case client := <-s.register:
			s.mu.Lock()
			s.clients[client] = true
			log.Printf("Client connected: %s (total: %d)", client.id, len(s.clients))
			s.mu.Unlock()
		case client := <-s.unregister:
			s.mu.Lock()
			if _, ok := s.clients[client]; ok {
				delete(s.clients, client)
				close(client.send)
				log.Printf("Client disconnected: %s (total: %d)", client.id, len(s.clients))
			}
			s.mu.Unlock()
		}
	}
}

func (s *Server) broadcastUpdates() {
	for update := range s.queue.Receive() {
		log.Printf("Update added: %v", update.Data)
		message := fmt.Sprintf("data: %s\n\n", update.Stringify())
		s.mu.RLock()
		for client := range s.clients {
			select {
			case client.send <- message:
			default:
				// Client buffer full, skip
			}
		}
		s.mu.RUnlock()
	}
}

func (s *Server) sseHandler(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		jsonError(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	clientID := r.Header.Get("SSE-Client-ID")
	if clientID == "" {
		clientID = "anonymous"
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	client := &sseClient{
		id:   clientID,
		send: make(chan string, 10),
	}
	s.register <- client

	defer func() {
		s.unregister <- client
	}()

	for {
		select {
		case message, ok := <-client.send:
			if !ok {
				return
			}
			fmt.Fprint(w, message)
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func (s *Server) postEventHandler(w http.ResponseWriter, r *http.Request) {
	var event messaging.Event
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		jsonError(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	clientID := r.Header.Get("SSE-Client-ID")
	if clientID != "" {
		event.SenderID = clientID
	}

	log.Printf("Event received: senderId=%s, eventType=%s, data=%v", event.SenderID, event.EventType, event.Data)
	s.storage.Push(&event)
	w.WriteHeader(http.StatusAccepted)
}

func jsonError(w http.ResponseWriter, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}
