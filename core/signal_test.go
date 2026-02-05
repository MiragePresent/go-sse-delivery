package core

import (
	"encoding/json"
	"testing"
)

func TestSignal_JSONMarshal(t *testing.T) {
	tests := []struct {
		name     string
		signal   Signal
		expected string
	}{
		{
			name: "signal with string data",
			signal: Signal{
				ConnectionID: "conn-1",
				Type:         "message",
				Data:         "hello world",
			},
			expected: `{"connectionId":"conn-1","type":"message","data":"hello world"}`,
		},
		{
			name: "signal with map data",
			signal: Signal{
				ConnectionID: "conn-2",
				Type:         "update",
				Data:         map[string]string{"key": "value"},
			},
			expected: `{"connectionId":"conn-2","type":"update","data":{"key":"value"}}`,
		},
		{
			name: "signal with empty fields",
			signal: Signal{
				ConnectionID: "",
				Type:         "",
				Data:         nil,
			},
			expected: `{"connectionId":"","type":"","data":null}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := json.Marshal(tt.signal)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if string(result) != tt.expected {
				t.Errorf("got %s, want %s", string(result), tt.expected)
			}
		})
	}
}

func TestSignal_JSONUnmarshal(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expected  Signal
		expectErr bool
	}{
		{
			name:  "valid signal with string data",
			input: `{"connectionId":"conn-1","type":"message","data":"hello"}`,
			expected: Signal{
				ConnectionID: "conn-1",
				Type:         "message",
				Data:         "hello",
			},
		},
		{
			name:  "valid signal with nested data",
			input: `{"connectionId":"conn-2","type":"update","data":{"nested":"value"}}`,
			expected: Signal{
				ConnectionID: "conn-2",
				Type:         "update",
				Data:         map[string]interface{}{"nested": "value"},
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
			var signal Signal
			err := json.Unmarshal([]byte(tt.input), &signal)
			if tt.expectErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if signal.ConnectionID != tt.expected.ConnectionID {
				t.Errorf("ConnectionID: got %s, want %s", signal.ConnectionID, tt.expected.ConnectionID)
			}
			if signal.Type != tt.expected.Type {
				t.Errorf("Type: got %s, want %s", signal.Type, tt.expected.Type)
			}
		})
	}
}
