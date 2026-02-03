package messaging

type UpdatesQueue interface {
	Send(update *Update)
	Receive() <-chan *Update
}

type ChannelQueue struct {
	updates chan *Update
}

func NewChannelQueue(bufferSize int) *ChannelQueue {
	return &ChannelQueue{
		updates: make(chan *Update, bufferSize),
	}
}

func (q *ChannelQueue) Send(update *Update) {
	q.updates <- update
}

func (q *ChannelQueue) Receive() <-chan *Update {
	return q.updates
}
