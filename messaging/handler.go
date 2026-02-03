package messaging

type Handler interface {
	Handle(event *Event) *Update
}

type EchoHandler struct{}

func (h *EchoHandler) Handle(event *Event) *Update {
	return &Update{Data: event.Data}
}
