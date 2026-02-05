package transport

import (
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/miragepresent/go-sse-delivery/core"
	"github.com/miragepresent/go-sse-delivery/identity"
)

type connection struct {
	connectionID string
	send         chan string
}

type HttpHandler struct {
	connections           map[string]*connection
	connected             chan *connection
	disconnected          chan *connection
	updates               <-chan *core.Update
	connectionIDResolver  identity.ConnectionIDResolver
	connectionIDGenerator identity.ConnectionIDGenerator
	mu                    sync.RWMutex
}

func NewCustomSseHandler(
	updates <-chan *core.Update,
	generator identity.ConnectionIDGenerator,
	resolver identity.ConnectionIDResolver,
) *HttpHandler {
	h := &HttpHandler{
		connections:           make(map[string]*connection),
		connected:             make(chan *connection),
		disconnected:          make(chan *connection),
		updates:               updates,
		connectionIDGenerator: generator,
		connectionIDResolver:  resolver,
		mu:                    sync.RWMutex{},
	}

	go h.manageConnections()

	return h
}

func NewSseHandler(updates <-chan *core.Update) *HttpHandler {
	return NewCustomSseHandler(
		updates,
		identity.DefaultConnectionIDGenerator,
		identity.DefaultConnectionIDResolver,
	)
}

func (h *HttpHandler) manageConnections() {

	for {
		select {
		case conn := <-h.connected:
			h.mu.Lock()
			log.Printf("connection %s established. total connections: %d\n", conn.connectionID, len(h.connections)+1)
			h.connections[conn.connectionID] = conn
			h.mu.Unlock()
		case disconn := <-h.disconnected:
			h.mu.Lock()
			log.Printf("connection %s closed. total connections: %d\n", disconn.connectionID, len(h.connections)-1)
			delete(h.connections, disconn.connectionID)
			close(disconn.send)
			h.mu.Unlock()
		case upd := <-h.updates:
			switch upd.Mode {
			case core.Broadcast:
				h.broadcastUpdate(upd)
			case core.Targeted:
				h.sendToTarget(upd)
			}
		}
	}
}

func (h *HttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	connectionID := h.connectionIDResolver(r)
	if connectionID == "" {
		connectionID = h.connectionIDGenerator()
		fmt.Fprintf(w, "event: connected\ndata: %s\n\n", connectionID)
		flusher.Flush()
	}

	log.Printf("SSE connection established: %s\n", connectionID)

	conn := &connection{
		connectionID: connectionID,
		send:         make(chan string, 10),
	}
	h.connected <- conn

	defer func() {
		h.disconnected <- conn
	}()

	for msg := range conn.send {
		fmt.Fprintf(w, "data: %s\n\n", msg)
		flusher.Flush()
	}
}

func (h *HttpHandler) broadcastUpdate(upd *core.Update) {
	h.mu.RLock()
	total := len(h.connections)
	log.Printf("broadcasting update to %d connection(s)", total)
	pool := make([]*connection, 0, total)
	for _, conn := range h.connections {
		pool = append(pool, conn)
	}
	h.mu.RUnlock()

	for _, conn := range pool {
		select {
		case conn.send <- upd.Stringify():
		default:
			log.Printf("connection %s is too slow (buffer full). update dropped", conn.connectionID)
		}
	}
}

func (h *HttpHandler) sendToTarget(upd *core.Update) {
	if len(upd.Connections) == 0 {
		log.Printf("no connectionID specified for targeted update")
		return
	}

	h.mu.RLock()
	conn, ok := h.connections[upd.Connections[0]]
	h.mu.RUnlock()

	if ok {
		select {
		case conn.send <- upd.Stringify():
		default:
			log.Printf("connection %s is too slow (buffer full). update dropped", conn.connectionID)
		}
		return
	}

	log.Printf("cannot send update to connection %s. connection closed", upd.Connections[0])
}
