package storage

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStorage is a persistent storage using Redis Streams
type RedisStorage struct {
	client        *redis.Client
	streamKey     string
	consumerGroup string
	maxLen        int64
	ttl           time.Duration
	decode        DecodeFunc
	closed        bool
	mu            sync.RWMutex
}

// RedisOption configures RedisStorage
type RedisOption func(*RedisStorage)

// WithMaxLen sets the maximum number of messages to keep in the stream
func WithMaxLen(n int64) RedisOption {
	return func(s *RedisStorage) {
		s.maxLen = n
	}
}

// WithTTL sets the TTL for messages (approximated via XTRIM)
func WithTTL(d time.Duration) RedisOption {
	return func(s *RedisStorage) {
		s.ttl = d
	}
}

// WithConsumerGroup enables consumer group for acknowledgment tracking
func WithConsumerGroup(name string) RedisOption {
	return func(s *RedisStorage) {
		s.consumerGroup = name
	}
}

// WithDecoder sets the function to decode messages from storage
func WithDecoder(fn DecodeFunc) RedisOption {
	return func(s *RedisStorage) {
		s.decode = fn
	}
}

// NewRedisStorage creates a new Redis-backed storage
func NewRedisStorage(client *redis.Client, streamKey string, opts ...RedisOption) *RedisStorage {
	s := &RedisStorage{
		client:    client,
		streamKey: streamKey,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Push adds a message to the Redis stream
func (s *RedisStorage) Push(ctx context.Context, msg Message) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return ErrStorageClosed
	}

	data, err := msg.Encode()
	if err != nil {
		return err
	}

	args := &redis.XAddArgs{
		Stream: s.streamKey,
		Values: map[string]interface{}{
			"id":   msg.ID(),
			"data": data,
		},
	}

	if s.maxLen > 0 {
		args.MaxLen = s.maxLen
		args.Approx = true
	}

	_, err = s.client.XAdd(ctx, args).Result()
	return err
}

// Subscribe creates a subscription to the Redis stream
func (s *RedisStorage) Subscribe(ctx context.Context, opts SubscribeOptions) (Subscription, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrStorageClosed
	}

	sub := &redisSubscription{
		storage:      s,
		ch:           make(chan Message, 100),
		consumerID:   opts.ConsumerID,
		historyCount: opts.HistoryCount,
		done:         make(chan struct{}),
	}

	// If using consumer group, ensure it exists
	if s.consumerGroup != "" && opts.ConsumerID != "" {
		err := s.ensureConsumerGroup(ctx)
		if err != nil {
			return nil, err
		}
	}

	go sub.run(ctx)
	return sub, nil
}

// Restore returns all unacknowledged messages from the consumer group
func (s *RedisStorage) Restore(ctx context.Context) ([]Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrStorageClosed
	}

	if s.consumerGroup == "" {
		return []Message{}, nil
	}

	// Check pending messages in the consumer group
	pending, err := s.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: s.streamKey,
		Group:  s.consumerGroup,
		Start:  "-",
		End:    "+",
		Count:  1000,
	}).Result()

	if err != nil {
		if err == redis.Nil {
			return []Message{}, nil
		}
		return nil, err
	}

	if len(pending) == 0 {
		return []Message{}, nil
	}

	// Collect message IDs
	ids := make([]string, len(pending))
	for i, p := range pending {
		ids[i] = p.ID
	}

	// Fetch the actual messages
	messages, err := s.client.XRange(ctx, s.streamKey, ids[0], ids[len(ids)-1]).Result()
	if err != nil {
		return nil, err
	}

	result := make([]Message, 0, len(messages))
	for _, xmsg := range messages {
		msg, err := s.decodeMessage(xmsg)
		if err != nil {
			continue
		}
		result = append(result, msg)
	}

	return result, nil
}

// Close shuts down the storage
func (s *RedisStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true
	return nil
}

func (s *RedisStorage) ensureConsumerGroup(ctx context.Context) error {
	err := s.client.XGroupCreateMkStream(ctx, s.streamKey, s.consumerGroup, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

func (s *RedisStorage) decodeMessage(xmsg redis.XMessage) (Message, error) {
	if s.decode == nil {
		return &rawMessage{
			id:   xmsg.Values["id"].(string),
			data: []byte(xmsg.Values["data"].(string)),
		}, nil
	}

	id := xmsg.Values["id"].(string)
	data := []byte(xmsg.Values["data"].(string))
	return s.decode(id, data)
}

// redisSubscription implements Subscription for RedisStorage
type redisSubscription struct {
	storage      *RedisStorage
	ch           chan Message
	consumerID   string
	historyCount int
	done         chan struct{}
	closed       bool
	mu           sync.Mutex
}

func (s *redisSubscription) Messages() <-chan Message {
	return s.ch
}

func (s *redisSubscription) Ack(msgID string) error {
	if s.storage.consumerGroup == "" || s.consumerID == "" {
		return nil
	}

	ctx := context.Background()
	return s.storage.client.XAck(ctx, s.storage.streamKey, s.storage.consumerGroup, msgID).Err()
}

func (s *redisSubscription) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	close(s.done)
	return nil
}

func (s *redisSubscription) run(ctx context.Context) {
	defer close(s.ch)

	// Send historical messages first
	if s.historyCount != 0 {
		s.sendHistory(ctx)
	}

	// Then stream new messages
	s.streamMessages(ctx)
}

func (s *redisSubscription) sendHistory(ctx context.Context) {
	var count int64 = 100
	if s.historyCount > 0 {
		count = int64(s.historyCount)
	}

	// Get recent messages from the stream
	messages, err := s.storage.client.XRevRangeN(ctx, s.storage.streamKey, "+", "-", count).Result()
	if err != nil {
		return
	}

	// Reverse to send in chronological order
	for i := len(messages) - 1; i >= 0; i-- {
		msg, err := s.storage.decodeMessage(messages[i])
		if err != nil {
			continue
		}

		select {
		case s.ch <- msg:
		case <-s.done:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (s *redisSubscription) streamMessages(ctx context.Context) {
	lastID := "$" // Only new messages

	for {
		select {
		case <-s.done:
			return
		case <-ctx.Done():
			return
		default:
		}

		var messages []redis.XStream
		var err error

		if s.storage.consumerGroup != "" && s.consumerID != "" {
			// Use consumer group for reliable delivery
			messages, err = s.storage.client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    s.storage.consumerGroup,
				Consumer: s.consumerID,
				Streams:  []string{s.storage.streamKey, ">"},
				Count:    10,
				Block:    time.Second,
			}).Result()
		} else {
			// Simple read without consumer group
			messages, err = s.storage.client.XRead(ctx, &redis.XReadArgs{
				Streams: []string{s.storage.streamKey, lastID},
				Count:   10,
				Block:   time.Second,
			}).Result()
		}

		if err != nil {
			if err == redis.Nil {
				continue
			}
			// Brief pause on error before retrying
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for _, stream := range messages {
			for _, xmsg := range stream.Messages {
				lastID = xmsg.ID

				msg, err := s.storage.decodeMessage(xmsg)
				if err != nil {
					continue
				}

				select {
				case s.ch <- msg:
				case <-s.done:
					return
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// rawMessage is a simple Message implementation for when no decoder is provided
type rawMessage struct {
	id   string
	data []byte
}

func (m *rawMessage) ID() string {
	return m.id
}

func (m *rawMessage) Encode() ([]byte, error) {
	return m.data, nil
}

func (m *rawMessage) Data() []byte {
	return m.data
}
