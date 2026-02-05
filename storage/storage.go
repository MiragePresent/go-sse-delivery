package storage

import "context"

// Message represents a storable item (Signal or Update)
type Message interface {
	ID() string
	Encode() ([]byte, error)
}

// DecodeFunc decodes bytes back into a Message
type DecodeFunc func(id string, data []byte) (Message, error)

// Storage handles message persistence and delivery
type Storage interface {
	// Push adds a new message to the storage
	Push(ctx context.Context, msg Message) error

	// Subscribe returns a subscription for receiving messages
	Subscribe(ctx context.Context, opts SubscribeOptions) (Subscription, error)

	// Restore returns all unacknowledged messages (for startup recovery)
	// Returns empty slice if acknowledgment tracking is disabled
	Restore(ctx context.Context) ([]Message, error)

	// Close gracefully shuts down the storage
	Close() error
}

// SubscribeOptions configures subscription behavior
type SubscribeOptions struct {
	// HistoryCount specifies how many historical messages to receive first
	// 0 means no history, -1 means all available history
	HistoryCount int

	// ConsumerID identifies this consumer for acknowledgment tracking
	// Empty string disables acknowledgment tracking
	ConsumerID string
}

// Subscription represents an active subscription to a storage
type Subscription interface {
	// Messages returns a channel for receiving messages
	Messages() <-chan Message

	// Ack acknowledges a message as processed
	// No-op if acknowledgment tracking is disabled
	Ack(msgID string) error

	// Close unsubscribes and cleans up resources
	Close() error
}
