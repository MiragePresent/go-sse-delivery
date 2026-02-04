package transport

import (
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/miragepresent/go-sse-delivery/core"
	"github.com/miragepresent/go-sse-delivery/identity"
)

// Individual SSE client connection
type client struct {
	clientId string
	send     chan string
}

type HttpHandler struct {
	clients        map[*client]bool
	connected      chan *client
	disconnected   chan *client
	updates        <-chan *core.Update
	clientResolver identity.ClientIdResolver
	mu             sync.RWMutex
}

func (h *HttpHandler) manageConnections() {

	for {
		select {
		case conn := <-h.connected:
			h.mu.Lock()
			log.Printf("client %s connected. number of connections: %d\n", conn.clientId, len(h.clients)+1)
			h.clients[conn] = true
			h.mu.Unlock()
		case disconn := <-h.disconnected:
			h.mu.Lock()
			log.Printf("client %s disconnected. number of connections: %d\n", disconn.clientId, len(h.clients)-1)
			delete(h.clients, disconn)
			close(disconn.send)
		case upd := <-h.updates:
			h.mu.RLock()
			cl := len(h.clients)
			log.Printf("broadcasting new message to %d active client(s)", cl)
			clientsPool := make([]*client, 0, cl)
			for c := range h.clients {
				clientsPool = append(clientsPool, c)
			}
			h.mu.RUnlock()

			for _, c := range clientsPool {
				select {
				case c.send <- upd.Stringify():
				default:
					log.Printf("client %s is too slow (buffer is full). update will not be delivered", c.clientId)
				}
			}
		}
	}

}

func (h *HttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("SSE endpoint got new connection\n")
	clientId, err := h.clientResolver(r)

	if err != nil || clientId == "" {
		log.Printf("cannot identify client ID. ignoring the connection. Error: %s", err)
		http.Error(w, "Unknown client ID. Disconnecting", http.StatusBadRequest)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	connection := &client{
		clientId: clientId,
		send:     make(chan string, 10),
	}
	h.connected <- connection

	defer func() {
		h.disconnected <- connection
	}()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	for msg := range connection.send {
		fmt.Fprintf(w, "data: %s", msg)
		flusher.Flush()
	}
}

func NewSseHandler(updates <-chan *core.Update) *HttpHandler {
	return NewSseHandlerWithClientResolver(updates, identity.DefaultClientIdResolver)
}

func NewSseHandlerWithClientResolver(updates <-chan *core.Update, resolver identity.ClientIdResolver) *HttpHandler {
	h := &HttpHandler{
		clients:        make(map[*client]bool),
		connected:      make(chan *client),
		disconnected:   make(chan *client),
		updates:        updates,
		clientResolver: resolver,
		mu:             sync.RWMutex{},
	}

	go h.manageConnections()

	return h
}
