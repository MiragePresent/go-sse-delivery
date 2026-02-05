package core

import (
	"encoding/json"

	"github.com/google/uuid"
)

type Signal struct {
	id           string
	ConnectionID string      `json:"connectionId"`
	Type         string      `json:"type"`
	Data         interface{} `json:"data"`
}

// NewSignal creates a new Signal with a generated ID
func NewSignal(connectionID, signalType string, data interface{}) *Signal {
	return &Signal{
		id:           uuid.New().String(),
		ConnectionID: connectionID,
		Type:         signalType,
		Data:         data,
	}
}

// ID returns the signal's unique identifier
func (s *Signal) ID() string {
	if s.id == "" {
		s.id = uuid.New().String()
	}
	return s.id
}

// Encode serializes the signal to JSON
func (s *Signal) Encode() ([]byte, error) {
	return json.Marshal(s)
}

// DecodeSignal decodes bytes back into a Signal
func DecodeSignal(id string, data []byte) (*Signal, error) {
	var s Signal
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	s.id = id
	return &s, nil
}
