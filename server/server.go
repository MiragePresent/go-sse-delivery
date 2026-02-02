package server

import (
	"fmt"
	"io"
	"net/http"
	"sync"
)

type Server struct {
	clients    map[chan string]bool
	register   chan chan string
	unregister chan chan string
	broadcast  chan string
	mu         sync.RWMutex
}

func NewServer() *Server {
	return &Server{
		clients:    make(map[chan string]bool),
		register:   make(chan chan string),
		unregister: make(chan chan string),
		broadcast:  make(chan string),
	}
}

func (s *Server) Start() {
	go s.run()

	http.HandleFunc("GET /events", s.sseHandler)
	http.HandleFunc("POST /events", s.postEventHandler)

	fmt.Println("Server starting on :8080")
	http.ListenAndServe(":8080", nil)
}

func (s *Server) run() {
	for {
		select {
		case client := <-s.register:
			s.mu.Lock()
			s.clients[client] = true
			s.mu.Unlock()
		case client := <-s.unregister:
			s.mu.Lock()
			if _, ok := s.clients[client]; ok {
				delete(s.clients, client)
				close(client)
			}
			s.mu.Unlock()
		case message := <-s.broadcast:
			s.mu.RLock()
			for client := range s.clients {
				select {
				case client <- message:
				default:
					// Client buffer full, skip
				}
			}
			s.mu.RUnlock()
		}
	}
}

func (s *Server) sseHandler(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	client := make(chan string, 10)
	s.register <- client

	defer func() {
		s.unregister <- client
	}()

	for {
		select {
		case message, ok := <-client:
			if !ok {
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", message)
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func (s *Server) postEventHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	s.broadcast <- string(body)
	w.WriteHeader(http.StatusAccepted)
}
