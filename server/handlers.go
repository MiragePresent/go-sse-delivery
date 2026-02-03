package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/miragepresent/go-sse-delivery/messaging"
)

type sseClient struct {
	id   string
	send chan string
}

type Handlers struct {
	clients    map[*sseClient]bool
	register   chan *sseClient
	unregister chan *sseClient
	ingress    chan *messaging.Event
	updates    chan *messaging.Update
	mu         sync.RWMutex
}

func NewHandlers(ingress chan *messaging.Event, updates chan *messaging.Update) *Handlers {
	return &Handlers{
		clients:    make(map[*sseClient]bool),
		register:   make(chan *sseClient),
		unregister: make(chan *sseClient),
		ingress:    ingress,
		updates:    updates,
	}
}

func (h *Handlers) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			log.Printf("Client connected: %s (total: %d)", client.id, len(h.clients))
			h.mu.Unlock()
		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
				log.Printf("Client disconnected: %s (total: %d)", client.id, len(h.clients))
			}
			h.mu.Unlock()
		case update := <-h.updates:
			log.Printf("Update added: %v", update.Data)
			message := fmt.Sprintf("data: %s\n\n", update.Stringify())
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					// Client buffer full, skip
				}
			}
			h.mu.RUnlock()
		}
	}
}

func (h *Handlers) DeliverEndpointHandler(w http.ResponseWriter, r *http.Request) {
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
	h.register <- client

	defer func() {
		h.unregister <- client
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

func (h *Handlers) NewEventsHandler(w http.ResponseWriter, r *http.Request) {
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
	h.ingress <- &event
	w.WriteHeader(http.StatusAccepted)
}

func jsonError(w http.ResponseWriter, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}
