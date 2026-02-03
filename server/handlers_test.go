package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/miragepresent/go-sse-delivery/messaging"
)

func TestNewHandlers(t *testing.T) {
	ingress := make(chan *messaging.Event)
	updates := make(chan *messaging.Update)

	h := NewHandlers(ingress, updates)

	if h.clients == nil {
		t.Error("clients map not initialized")
	}
	if h.register == nil {
		t.Error("register channel not initialized")
	}
	if h.unregister == nil {
		t.Error("unregister channel not initialized")
	}
	if h.ingress != ingress {
		t.Error("ingress channel not set correctly")
	}
	if h.updates != updates {
		t.Error("updates channel not set correctly")
	}
}

func TestHandlers_Run_ClientRegistration(t *testing.T) {
	ingress := make(chan *messaging.Event)
	updates := make(chan *messaging.Update)
	h := NewHandlers(ingress, updates)

	go h.Run()

	client := &sseClient{
		id:   "test-client",
		send: make(chan string, 10),
	}

	h.register <- client

	time.Sleep(10 * time.Millisecond)

	h.mu.RLock()
	if !h.clients[client] {
		t.Error("client was not registered")
	}
	clientCount := len(h.clients)
	h.mu.RUnlock()

	if clientCount != 1 {
		t.Errorf("expected 1 client, got %d", clientCount)
	}
}

func TestHandlers_Run_ClientUnregistration(t *testing.T) {
	ingress := make(chan *messaging.Event)
	updates := make(chan *messaging.Update)
	h := NewHandlers(ingress, updates)

	go h.Run()

	client := &sseClient{
		id:   "test-client",
		send: make(chan string, 10),
	}

	h.register <- client
	time.Sleep(10 * time.Millisecond)

	h.unregister <- client
	time.Sleep(10 * time.Millisecond)

	h.mu.RLock()
	clientCount := len(h.clients)
	h.mu.RUnlock()

	if clientCount != 0 {
		t.Errorf("expected 0 clients, got %d", clientCount)
	}

	select {
	case _, ok := <-client.send:
		if ok {
			t.Error("client send channel should be closed")
		}
	default:
		t.Error("client send channel should be closed")
	}
}

func TestHandlers_Run_Broadcast(t *testing.T) {
	ingress := make(chan *messaging.Event)
	updates := make(chan *messaging.Update)
	h := NewHandlers(ingress, updates)

	go h.Run()

	client1 := &sseClient{id: "client-1", send: make(chan string, 10)}
	client2 := &sseClient{id: "client-2", send: make(chan string, 10)}

	h.register <- client1
	h.register <- client2
	time.Sleep(10 * time.Millisecond)

	updates <- &messaging.Update{Data: "broadcast message"}

	time.Sleep(10 * time.Millisecond)

	expected := "data: broadcast message\n\n"

	select {
	case msg := <-client1.send:
		if msg != expected {
			t.Errorf("client1: got %q, want %q", msg, expected)
		}
	default:
		t.Error("client1 did not receive message")
	}

	select {
	case msg := <-client2.send:
		if msg != expected {
			t.Errorf("client2: got %q, want %q", msg, expected)
		}
	default:
		t.Error("client2 did not receive message")
	}
}

type mockFlusher struct {
	http.ResponseWriter
	headers http.Header
	written []byte
	flushed int
}

func newMockFlusher() *mockFlusher {
	return &mockFlusher{
		headers: make(http.Header),
	}
}

func (m *mockFlusher) Header() http.Header {
	return m.headers
}

func (m *mockFlusher) Write(data []byte) (int, error) {
	m.written = append(m.written, data...)
	return len(data), nil
}

func (m *mockFlusher) WriteHeader(statusCode int) {}

func (m *mockFlusher) Flush() {
	m.flushed++
}

func TestDeliverEndpointHandler_Headers(t *testing.T) {
	ingress := make(chan *messaging.Event)
	updates := make(chan *messaging.Update)
	h := NewHandlers(ingress, updates)

	go h.Run()

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/live-updates", nil).WithContext(ctx)
	req.Header.Set("SSE-Client-ID", "test-client")

	w := newMockFlusher()

	done := make(chan struct{})
	go func() {
		h.DeliverEndpointHandler(w, req)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	<-done

	if w.headers.Get("Content-Type") != "text/event-stream" {
		t.Errorf("Content-Type: got %q, want %q", w.headers.Get("Content-Type"), "text/event-stream")
	}
	if w.headers.Get("Cache-Control") != "no-cache" {
		t.Errorf("Cache-Control: got %q, want %q", w.headers.Get("Cache-Control"), "no-cache")
	}
	if w.headers.Get("Connection") != "keep-alive" {
		t.Errorf("Connection: got %q, want %q", w.headers.Get("Connection"), "keep-alive")
	}
}

func TestDeliverEndpointHandler_AnonymousClient(t *testing.T) {
	ingress := make(chan *messaging.Event)
	updates := make(chan *messaging.Update)
	h := NewHandlers(ingress, updates)

	go h.Run()

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/live-updates", nil).WithContext(ctx)
	// No SSE-Client-ID header set

	w := newMockFlusher()

	done := make(chan struct{})
	go func() {
		h.DeliverEndpointHandler(w, req)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)

	h.mu.RLock()
	var clientID string
	for client := range h.clients {
		clientID = client.id
		break
	}
	h.mu.RUnlock()

	cancel()
	<-done

	if clientID != "anonymous" {
		t.Errorf("client ID: got %q, want %q", clientID, "anonymous")
	}
}

// nonFlusher is a ResponseWriter that does NOT implement http.Flusher
type nonFlusher struct {
	headers    http.Header
	statusCode int
	body       bytes.Buffer
}

func (n *nonFlusher) Header() http.Header {
	if n.headers == nil {
		n.headers = make(http.Header)
	}
	return n.headers
}

func (n *nonFlusher) Write(data []byte) (int, error) {
	return n.body.Write(data)
}

func (n *nonFlusher) WriteHeader(statusCode int) {
	n.statusCode = statusCode
}

func TestDeliverEndpointHandler_NonFlusher(t *testing.T) {
	ingress := make(chan *messaging.Event)
	updates := make(chan *messaging.Update)
	h := NewHandlers(ingress, updates)

	req := httptest.NewRequest(http.MethodGet, "/live-updates", nil)
	w := &nonFlusher{}

	h.DeliverEndpointHandler(w, req)

	if w.statusCode != http.StatusInternalServerError {
		t.Errorf("status code: got %d, want %d", w.statusCode, http.StatusInternalServerError)
	}

	var response map[string]string
	json.Unmarshal(w.body.Bytes(), &response)
	if response["error"] != "SSE not supported" {
		t.Errorf("error message: got %q, want %q", response["error"], "SSE not supported")
	}
}

func TestDeliverEndpointHandler_ReceivesMessages(t *testing.T) {
	ingress := make(chan *messaging.Event)
	updates := make(chan *messaging.Update)
	h := NewHandlers(ingress, updates)

	go h.Run()

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/live-updates", nil).WithContext(ctx)
	req.Header.Set("SSE-Client-ID", "test-client")

	w := newMockFlusher()

	done := make(chan struct{})
	go func() {
		h.DeliverEndpointHandler(w, req)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)

	updates <- &messaging.Update{Data: "test message"}

	time.Sleep(20 * time.Millisecond)

	cancel()
	<-done

	expected := "data: test message\n\n"
	if !strings.Contains(string(w.written), expected) {
		t.Errorf("written: got %q, want to contain %q", string(w.written), expected)
	}
	if w.flushed < 1 {
		t.Error("Flush was not called")
	}
}

func TestNewEventsHandler_ValidEvent(t *testing.T) {
	ingress := make(chan *messaging.Event, 1)
	updates := make(chan *messaging.Update)
	h := NewHandlers(ingress, updates)

	body := `{"senderId":"sender-1","eventType":"test","data":"hello"}`
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()

	h.NewEventsHandler(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("status code: got %d, want %d", w.Code, http.StatusAccepted)
	}

	select {
	case event := <-ingress:
		if event.SenderID != "sender-1" {
			t.Errorf("SenderID: got %q, want %q", event.SenderID, "sender-1")
		}
		if event.EventType != "test" {
			t.Errorf("EventType: got %q, want %q", event.EventType, "test")
		}
		if event.Data != "hello" {
			t.Errorf("Data: got %v, want %v", event.Data, "hello")
		}
	default:
		t.Error("event was not sent to ingress channel")
	}
}

func TestNewEventsHandler_WithClientIDHeader(t *testing.T) {
	ingress := make(chan *messaging.Event, 1)
	updates := make(chan *messaging.Update)
	h := NewHandlers(ingress, updates)

	body := `{"eventType":"test","data":"hello"}`
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("SSE-Client-ID", "header-client")

	w := httptest.NewRecorder()

	h.NewEventsHandler(w, req)

	select {
	case event := <-ingress:
		if event.SenderID != "header-client" {
			t.Errorf("SenderID: got %q, want %q", event.SenderID, "header-client")
		}
	default:
		t.Error("event was not sent to ingress channel")
	}
}

func TestNewEventsHandler_InvalidJSON(t *testing.T) {
	ingress := make(chan *messaging.Event, 1)
	updates := make(chan *messaging.Update)
	h := NewHandlers(ingress, updates)

	body := `{invalid json}`
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()

	h.NewEventsHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status code: got %d, want %d", w.Code, http.StatusBadRequest)
	}

	var response map[string]string
	json.Unmarshal(w.Body.Bytes(), &response)
	if response["error"] != "Invalid JSON" {
		t.Errorf("error message: got %q, want %q", response["error"], "Invalid JSON")
	}

	select {
	case <-ingress:
		t.Error("event should not be sent for invalid JSON")
	default:
		// Expected: no event
	}
}

func TestJsonError(t *testing.T) {
	tests := []struct {
		name           string
		message        string
		status         int
		expectedStatus int
		expectedBody   string
	}{
		{
			name:           "bad request",
			message:        "Invalid input",
			status:         http.StatusBadRequest,
			expectedStatus: http.StatusBadRequest,
			expectedBody:   `{"error":"Invalid input"}`,
		},
		{
			name:           "internal error",
			message:        "Something went wrong",
			status:         http.StatusInternalServerError,
			expectedStatus: http.StatusInternalServerError,
			expectedBody:   `{"error":"Something went wrong"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			jsonError(w, tt.message, tt.status)

			if w.Code != tt.expectedStatus {
				t.Errorf("status code: got %d, want %d", w.Code, tt.expectedStatus)
			}
			if w.Header().Get("Content-Type") != "application/json" {
				t.Errorf("Content-Type: got %q, want %q", w.Header().Get("Content-Type"), "application/json")
			}

			body := strings.TrimSpace(w.Body.String())
			if body != tt.expectedBody {
				t.Errorf("body: got %q, want %q", body, tt.expectedBody)
			}
		})
	}
}
