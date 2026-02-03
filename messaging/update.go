package messaging

import "encoding/json"

type Update struct {
	Data interface{}
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
