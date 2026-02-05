package transport

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/miragepresent/go-sse-delivery/core"
	"github.com/miragepresent/go-sse-delivery/identity"
)

type ConnectionsUpdateHandler func(connId string, countActive int)

type connection struct {
	connectionID string
	send         chan []byte
}

type HttpHandler struct {
	pool                  *ConnectionPool
	connected             chan *connection
	disconnected          chan *connection
	updates               <-chan *core.Update
	connectionIDGenerator identity.ConnectionIDGenerator
	connectionIDResolver  identity.ConnectionIDResolver
	onConnected           ConnectionsUpdateHandler
	onDisconnected        ConnectionsUpdateHandler
	onError               core.ErrorHandler
}

func NewSseHandler(updates <-chan *core.Update, options ...Option) *HttpHandler {
	h := &HttpHandler{
		pool:                  NewConnectionPool(),
		connected:             make(chan *connection),
		disconnected:          make(chan *connection),
		updates:               updates,
		connectionIDGenerator: identity.DefaultConnectionIDGenerator,
		connectionIDResolver:  identity.DefaultConnectionIDResolver,
	}

	// Call option setters
	for _, opt := range options {
		opt(h)
	}

	go h.manageConnections()

	return h
}

// Options setters
type Option func(h *HttpHandler)

func WithConnectionIdGenerator(callback identity.ConnectionIDGenerator) Option {
	return func(h *HttpHandler) {
		h.connectionIDGenerator = callback
	}
}
func WithConnectionIdResolver(callback identity.ConnectionIDResolver) Option {
	return func(h *HttpHandler) {
		h.connectionIDResolver = callback
	}
}
func OnConnected(callback ConnectionsUpdateHandler) Option {
	return func(h *HttpHandler) {
		h.onConnected = callback
	}
}
func OnDisconnected(callback ConnectionsUpdateHandler) Option {
	return func(h *HttpHandler) {
		h.onDisconnected = callback
	}
}
func OnError(callback core.ErrorHandler) Option {
	return func(h *HttpHandler) {
		h.onError = callback
	}
}

func (h *HttpHandler) manageConnections() {
	for {
		select {
		case conn := <-h.connected:
			h.pool.Add(conn)
			if h.onConnected != nil {
				h.onConnected(conn.connectionID, h.pool.Count())
			}
		case disconn := <-h.disconnected:
			h.pool.Close(disconn.connectionID)
			if h.onDisconnected != nil {
				h.onDisconnected(disconn.connectionID, h.pool.Count())
			}
		case upd := <-h.updates:
			switch upd.Mode {
			case core.Broadcast:
				h.broadcastUpdate(upd)
			case core.Target:
				h.sendToConnections(upd)
			default:
				h.reportError(core.ErrUnknownDeliveryMode)
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

	conn := &connection{
		connectionID: connectionID,
		send:         make(chan []byte, 10),
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
	data, err := json.Marshal(upd.Data)
	if err != nil {
		h.reportError(core.SerializationError(err))
		return
	}

	connections := h.pool.All()
	for _, conn := range connections {
		select {
		case conn.send <- data:
		default:
			h.reportError(core.BufferFullError(conn.connectionID))
		}
	}
}

func (h *HttpHandler) sendToConnections(upd *core.Update) {
	if len(upd.Connections) == 0 {
		h.reportError(core.ErrNoConnections)
		return
	}

	data, err := json.Marshal(upd.Data)
	if err != nil {
		h.reportError(core.SerializationError(err))
		return
	}

	for _, connID := range upd.Connections {
		conn := h.pool.Get(connID)
		if conn == nil {
			h.reportError(core.ConnectionNotFoundError(connID))
			continue
		}
		select {
		case conn.send <- data:
		default:
			h.reportError(core.BufferFullError(conn.connectionID))
		}
	}
}

func (h *HttpHandler) reportError(err error) {
	if h.onError != nil {
		h.onError(err)
	}
}
