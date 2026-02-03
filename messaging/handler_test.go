package messaging

import (
	"reflect"
	"sync"
	"testing"
)

func TestEchoHandler_Handle(t *testing.T) {
	handler := &EchoHandler{}

	tests := []struct {
		name     string
		event    *Event
		expected *Update
	}{
		{
			name: "string data",
			event: &Event{
				SenderID:  "client-1",
				EventType: "message",
				Data:      "hello",
			},
			expected: &Update{Data: "hello"},
		},
		{
			name: "map data",
			event: &Event{
				SenderID:  "client-2",
				EventType: "update",
				Data:      map[string]string{"key": "value"},
			},
			expected: &Update{Data: map[string]string{"key": "value"}},
		},
		{
			name: "nil data",
			event: &Event{
				SenderID:  "client-3",
				EventType: "empty",
				Data:      nil,
			},
			expected: &Update{Data: nil},
		},
		{
			name: "slice data",
			event: &Event{
				SenderID:  "client-4",
				EventType: "list",
				Data:      []int{1, 2, 3},
			},
			expected: &Update{Data: []int{1, 2, 3}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.Handle(tt.event)
			if !reflect.DeepEqual(result.Data, tt.expected.Data) {
				t.Errorf("got %v, want %v", result.Data, tt.expected.Data)
			}
		})
	}
}

func TestEchoHandler_ImplementsHandler(t *testing.T) {
	var _ Handler = (*EchoHandler)(nil)
}

type mockHandler struct {
	returnNil bool
	called    bool
	lastEvent *Event
	mu        sync.Mutex
}

func (m *mockHandler) Handle(event *Event) *Update {
	m.mu.Lock()
	m.called = true
	m.lastEvent = event
	m.mu.Unlock()
	if m.returnNil {
		return nil
	}
	return &Update{Data: "processed"}
}

func (m *mockHandler) wasCalled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.called
}

func TestMockHandler_ImplementsHandler(t *testing.T) {
	var _ Handler = (*mockHandler)(nil)
}
