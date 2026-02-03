package messaging

import "log"

type Processor struct {
	ingress chan *Event
	updates chan *Update
	handler Handler
}

func NewProcessor(ingress chan *Event, updates chan *Update, handler Handler) *Processor {
	return &Processor{
		ingress: ingress,
		updates: updates,
		handler: handler,
	}
}

func (p *Processor) Process() {
	for event := range p.ingress {
		log.Printf("Processing event: senderId=%s, eventType=%s", event.SenderID, event.EventType)
		update := p.handler.Handle(event)
		if update != nil {
			p.updates <- update
		}
	}
}
