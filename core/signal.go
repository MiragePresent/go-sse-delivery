package core

type Signal struct {
	SenderID string      `json:"senderId"`
	Type     string      `json:"type"`
	Data     interface{} `json:"data"`
}
