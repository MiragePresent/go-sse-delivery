package core

import (
	"log"
)

type Dispatcher struct {
	ingress  chan *Signal
	updates  chan *Update
	handlers map[string]Handler
}

func NewDispatcher(ingress chan *Signal, updates chan *Update) *Dispatcher {
	return &Dispatcher{
		ingress:  ingress,
		updates:  updates,
		handlers: map[string]Handler{},
	}
}

func (d *Dispatcher) Register(signalType string, handler Handler) {
	if d.handlers == nil {
		d.handlers = map[string]Handler{}
	}

	d.handlers[signalType] = handler
}

func (d *Dispatcher) Run() {
	for signal := range d.ingress {
		log.Printf("Dispatching signal: connectionId=%s, type=%s", signal.ConnectionID, signal.Type)
		h := d.getHandler(signal.Type)
		if h == nil {
			log.Printf("no handler found for signal type %s\n", signal.Type)
			continue
		}
		h.Handle(signal, d.updates)
	}
}

func (d *Dispatcher) getHandler(signalType string) Handler {
	if h, ok := d.handlers[signalType]; ok {
		return h
	}

	return nil
}
