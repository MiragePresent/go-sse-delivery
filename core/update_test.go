package core

import "testing"

func TestUpdate_Stringify(t *testing.T) {
	tests := []struct {
		name     string
		update   Update
		expected string
	}{
		{
			name:     "string data",
			update:   Update{Data: "hello world"},
			expected: "hello world",
		},
		{
			name:     "empty string data",
			update:   Update{Data: ""},
			expected: "",
		},
		{
			name:     "map data",
			update:   Update{Data: map[string]string{"key": "value"}},
			expected: `{"key":"value"}`,
		},
		{
			name:     "nested map data",
			update:   Update{Data: map[string]interface{}{"outer": map[string]string{"inner": "value"}}},
			expected: `{"outer":{"inner":"value"}}`,
		},
		{
			name:     "slice data",
			update:   Update{Data: []string{"a", "b", "c"}},
			expected: `["a","b","c"]`,
		},
		{
			name:     "integer data",
			update:   Update{Data: 42},
			expected: "42",
		},
		{
			name:     "float data",
			update:   Update{Data: 3.14},
			expected: "3.14",
		},
		{
			name:     "boolean data",
			update:   Update{Data: true},
			expected: "true",
		},
		{
			name:     "nil data",
			update:   Update{Data: nil},
			expected: "null",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.update.Stringify()
			if result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestUpdate_Stringify_Struct(t *testing.T) {
	type payload struct {
		Message string `json:"message"`
		Count   int    `json:"count"`
	}

	update := Update{Data: payload{Message: "test", Count: 5}}
	expected := `{"message":"test","count":5}`

	result := update.Stringify()
	if result != expected {
		t.Errorf("got %q, want %q", result, expected)
	}
}
