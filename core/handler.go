package core

import (
	"log"
	"reflect"
)

type Handler interface {
	Handle(s *Signal, updates chan *Update)
}

type EchoHandler struct{}

func (h *EchoHandler) Handle(signal *Signal, updates chan *Update) {
	select {
	case updates <- &Update{Data: signal.Data, Mode: Broadcast}:
	default:
		log.Printf("%s cannot publish update", reflect.TypeOf(h).String())
	}
}
