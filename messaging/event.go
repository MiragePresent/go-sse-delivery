package messaging

type Event struct {
	SenderID  string      `json:"senderId"`
	EventType string      `json:"eventType"`
	Data      interface{} `json:"data"`
}
