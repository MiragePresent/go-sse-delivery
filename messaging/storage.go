package messaging

type EventsStorage interface {
	Push(event *Event)
	Pop() <-chan *Event
}

type ChannelStorage struct {
	events chan *Event
}

func NewChannelStorage(bufferSize int) *ChannelStorage {
	return &ChannelStorage{
		events: make(chan *Event, bufferSize),
	}
}

func (s *ChannelStorage) Push(event *Event) {
	s.events <- event
}

func (s *ChannelStorage) Pop() <-chan *Event {
	return s.events
}
