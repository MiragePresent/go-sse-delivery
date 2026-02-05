package core

import (
	"encoding/json"

	"github.com/google/uuid"
)

type DeliveryMode int

const (
	Broadcast DeliveryMode = 0
	Target    DeliveryMode = 10
)

type Update struct {
	id          string
	Data        any          `json:"data"`
	Mode        DeliveryMode `json:"mode"`
	Connections []string     `json:"connections,omitempty"`
}

// NewUpdate creates a new Update with a generated ID
func NewUpdate(data any, mode DeliveryMode, connections ...string) *Update {
	return &Update{
		id:          uuid.New().String(),
		Data:        data,
		Mode:        mode,
		Connections: connections,
	}
}

// ID returns the update's unique identifier
func (u *Update) ID() string {
	if u.id == "" {
		u.id = uuid.New().String()
	}
	return u.id
}

// Encode serializes the update to JSON
func (u *Update) Encode() ([]byte, error) {
	return json.Marshal(u)
}

// DecodeUpdate decodes bytes back into an Update
func DecodeUpdate(id string, data []byte) (*Update, error) {
	var u Update
	if err := json.Unmarshal(data, &u); err != nil {
		return nil, err
	}
	u.id = id
	return &u, nil
}
