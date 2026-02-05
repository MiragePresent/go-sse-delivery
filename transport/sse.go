package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/miragepresent/go-sse-delivery/core"
	"github.com/miragepresent/go-sse-delivery/identity"
	"github.com/miragepresent/go-sse-delivery/storage"
)

type ConnectionsUpdateHandler func(connId string, countActive int)

type connection struct {
	connectionID string
	send         chan []byte
}

type HttpHandler struct {
	updates               storage.Storage
	pool                  *ConnectionPool
	connectionIDGenerator identity.ConnectionIDGenerator
	connectionIDResolver  identity.ConnectionIDResolver
	onConnected           ConnectionsUpdateHandler
	onDisconnected        ConnectionsUpdateHandler
	onError               core.ErrorHandler
	historyCount          int
}

func NewSseHandler(updates storage.Storage, options ...Option) *HttpHandler {
	h := &HttpHandler{
		updates:               updates,
		pool:                  NewConnectionPool(),
		connectionIDGenerator: identity.DefaultConnectionIDGenerator,
		connectionIDResolver:  identity.DefaultConnectionIDResolver,
		historyCount:          0,
	}

	for _, opt := range options {
		opt(h)
	}

	return h
}

// Options setters
type Option func(h *HttpHandler)

func WithHistoryCount(count int) Option {
	return func(h *HttpHandler) {
		h.historyCount = count
	}
}

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

	h.pool.Add(conn)
	if h.onConnected != nil {
		h.onConnected(conn.connectionID, h.pool.Count())
	}

	defer func() {
		h.pool.Close(conn.connectionID)
		if h.onDisconnected != nil {
			h.onDisconnected(conn.connectionID, h.pool.Count())
		}
	}()

	// Subscribe to updates with history
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	sub, err := h.updates.Subscribe(ctx, storage.SubscribeOptions{
		HistoryCount: h.historyCount,
	})
	if err != nil {
		h.reportError(err)
		return
	}
	defer sub.Close()

	for {
		select {
		case <-r.Context().Done():
			return
		case msg, ok := <-sub.Messages():
			if !ok {
				return
			}

			update, ok := msg.(*core.Update)
			if !ok {
				continue
			}

			if !h.shouldSendToConnection(update, connectionID) {
				continue
			}

			data, err := json.Marshal(update.Data)
			if err != nil {
				h.reportError(core.SerializationError(err))
				continue
			}

			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}

func (h *HttpHandler) shouldSendToConnection(update *core.Update, connectionID string) bool {
	switch update.Mode {
	case core.Broadcast:
		return true
	case core.Target:
		for _, id := range update.Connections {
			if id == connectionID {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func (h *HttpHandler) reportError(err error) {
	if h.onError != nil {
		h.onError(err)
	}
}
