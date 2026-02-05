package core

import "encoding/json"

type DeliveryMode int

const (
	Broadcast DeliveryMode = 0
	Targeted  DeliveryMode = 1
)

type Update struct {
	Data        interface{}
	Mode        DeliveryMode
	Connections []string
}

func (u *Update) Stringify() string {
	switch v := u.Data.(type) {
	case string:
		return v
	default:
		bytes, _ := json.Marshal(v)
		return string(bytes)
	}
}
