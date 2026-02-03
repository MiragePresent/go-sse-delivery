package messaging

import (
	"encoding/json"
	"testing"
)

func TestEvent_JSONMarshal(t *testing.T) {
	tests := []struct {
		name     string
		event    Event
		expected string
	}{
		{
			name: "event with string data",
			event: Event{
				SenderID:  "client-1",
				EventType: "message",
				Data:      "hello world",
			},
			expected: `{"senderId":"client-1","eventType":"message","data":"hello world"}`,
		},
		{
			name: "event with map data",
			event: Event{
				SenderID:  "client-2",
				EventType: "update",
				Data:      map[string]string{"key": "value"},
			},
			expected: `{"senderId":"client-2","eventType":"update","data":{"key":"value"}}`,
		},
		{
			name: "event with empty fields",
			event: Event{
				SenderID:  "",
				EventType: "",
				Data:      nil,
			},
			expected: `{"senderId":"","eventType":"","data":null}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := json.Marshal(tt.event)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if string(result) != tt.expected {
				t.Errorf("got %s, want %s", string(result), tt.expected)
			}
		})
	}
}

func TestEvent_JSONUnmarshal(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expected  Event
		expectErr bool
	}{
		{
			name:  "valid event with string data",
			input: `{"senderId":"client-1","eventType":"message","data":"hello"}`,
			expected: Event{
				SenderID:  "client-1",
				EventType: "message",
				Data:      "hello",
			},
		},
		{
			name:  "valid event with nested data",
			input: `{"senderId":"client-2","eventType":"update","data":{"nested":"value"}}`,
			expected: Event{
				SenderID:  "client-2",
				EventType: "update",
				Data:      map[string]interface{}{"nested": "value"},
			},
		},
		{
			name:      "invalid json",
			input:     `{invalid}`,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var event Event
			err := json.Unmarshal([]byte(tt.input), &event)
			if tt.expectErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if event.SenderID != tt.expected.SenderID {
				t.Errorf("SenderID: got %s, want %s", event.SenderID, tt.expected.SenderID)
			}
			if event.EventType != tt.expected.EventType {
				t.Errorf("EventType: got %s, want %s", event.EventType, tt.expected.EventType)
			}
		})
	}
}
