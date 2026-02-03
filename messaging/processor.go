package messaging

type Processor struct {
	storage EventsStorage
	queue   UpdatesQueue
	handler Handler
}

func NewProcessor(storage EventsStorage, queue UpdatesQueue, handler Handler) *Processor {
	return &Processor{
		storage: storage,
		queue:   queue,
		handler: handler,
	}
}

func (p *Processor) Run() {
	for event := range p.storage.Pop() {
		update := p.handler.Handle(event)
		if update != nil {
			p.queue.Send(update)
		}
	}
}
