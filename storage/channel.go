package storage

import (
	"context"
	"sync"
)

// ChannelStorage is an in-memory storage using Go channels
// No persistence, no history support
type ChannelStorage struct {
	bufferSize  int
	subscribers []*channelSubscription
	mu          sync.RWMutex
	closed      bool
}

// ChannelOption configures ChannelStorage
type ChannelOption func(*ChannelStorage)

// WithBufferSize sets the buffer size for subscriber channels
func WithBufferSize(size int) ChannelOption {
	return func(s *ChannelStorage) {
		s.bufferSize = size
	}
}

// NewChannelStorage creates a new in-memory channel-based storage
func NewChannelStorage(opts ...ChannelOption) *ChannelStorage {
	s := &ChannelStorage{
		bufferSize:  100,
		subscribers: make([]*channelSubscription, 0),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Push sends a message to all subscribers
func (s *ChannelStorage) Push(ctx context.Context, msg Message) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return ErrStorageClosed
	}

	for _, sub := range s.subscribers {
		select {
		case sub.ch <- msg:
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Non-blocking: skip if buffer full
		}
	}
	return nil
}

// Subscribe creates a new subscription
// HistoryCount is ignored (no history available)
func (s *ChannelStorage) Subscribe(ctx context.Context, opts SubscribeOptions) (Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrStorageClosed
	}

	sub := &channelSubscription{
		ch:      make(chan Message, s.bufferSize),
		storage: s,
	}
	s.subscribers = append(s.subscribers, sub)
	return sub, nil
}

// Restore returns empty slice (no persistence)
func (s *ChannelStorage) Restore(ctx context.Context) ([]Message, error) {
	return []Message{}, nil
}

// Close shuts down the storage and all subscriptions
func (s *ChannelStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	for _, sub := range s.subscribers {
		close(sub.ch)
	}
	s.subscribers = nil
	return nil
}

func (s *ChannelStorage) removeSubscription(sub *channelSubscription) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, existing := range s.subscribers {
		if existing == sub {
			s.subscribers = append(s.subscribers[:i], s.subscribers[i+1:]...)
			break
		}
	}
}

// channelSubscription implements Subscription for ChannelStorage
type channelSubscription struct {
	ch      chan Message
	storage *ChannelStorage
	closed  bool
	mu      sync.Mutex
}

func (s *channelSubscription) Messages() <-chan Message {
	return s.ch
}

// Ack is a no-op for channel storage
func (s *channelSubscription) Ack(msgID string) error {
	return nil
}

func (s *channelSubscription) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	s.storage.removeSubscription(s)
	return nil
}
