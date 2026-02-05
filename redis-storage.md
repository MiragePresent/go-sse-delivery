# Redis Storage Feature

## Overview

Storage abstraction layer replacing direct channel communication for signals (ingress) and updates. Enables:

- **Signal persistence**: Survive service restarts without losing unprocessed signals
- **Audit trail**: Keep history of processed signals
- **Update replay**: Deliver historical updates to new SSE connections
- **Scalability**: Multiple service instances can share the same Redis backend

## Architecture

```
[SignalReceiver] --> [Storage] --> [Dispatcher] --> [Storage] --> [HttpHandler]
                     (signals)                      (updates)
```

Both ingress and updates use the same `Storage` interface. Each is a separate instance with its own configuration.

## Storage Interface

```go
// Message represents a storable item (Signal or Update)
type Message interface {
    ID() string
    Encode() ([]byte, error)
}

// DecodeFunc decodes bytes back into a Message
type DecodeFunc func(id string, data []byte) (Message, error)

// Storage handles message persistence and delivery
type Storage interface {
    Push(ctx context.Context, msg Message) error
    Subscribe(ctx context.Context, opts SubscribeOptions) (Subscription, error)
    Restore(ctx context.Context) ([]Message, error)
    Close() error
}

type SubscribeOptions struct {
    HistoryCount int    // 0=no history, -1=all available
    ConsumerID   string // empty=no acknowledgment tracking
}

type Subscription interface {
    Messages() <-chan Message
    Ack(msgID string) error
    Close() error
}
```

## Implementations

### ChannelStorage

In-memory storage using Go channels. No persistence, no history.

```go
signals := storage.NewChannelStorage()
updates := storage.NewChannelStorage(storage.WithBufferSize(200))
```

### RedisStorage

Persistent storage using Redis Streams.

```go
client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

signals := storage.NewRedisStorage(client, "sse:signals",
    storage.WithConsumerGroup("dispatchers"),
    storage.WithMaxLen(10000),
    storage.WithDecoder(func(id string, data []byte) (storage.Message, error) {
        return core.DecodeSignal(id, data)
    }),
)

updates := storage.NewRedisStorage(client, "sse:updates",
    storage.WithMaxLen(1000),
    storage.WithDecoder(func(id string, data []byte) (storage.Message, error) {
        return core.DecodeUpdate(id, data)
    }),
)
```

#### Redis Options

- `WithMaxLen(n int64)` - Limit stored messages (uses MAXLEN ~n for efficiency)
- `WithTTL(d time.Duration)` - Message expiration (future feature)
- `WithConsumerGroup(name string)` - Enable consumer groups for reliable processing
- `WithDecoder(fn DecodeFunc)` - Custom message decoder

## Usage

### Storage-Based Components

```go
// Dispatcher that reads from signals storage and writes to updates storage
dispatcher := core.NewStorageDispatcher(signals, updates,
    core.OnStorageDispatcherError(func(err error) {
        log.Printf("dispatcher error: %v\n", err)
    }),
)
dispatcher.Register("message", handler)
go dispatcher.Start(ctx)

// Signal receiver that pushes to storage
signalReceiver := transport.NewStorageSignalReceiver(signals,
    transport.OnStorageSignalReceived(func(connID, signalType string) {
        log.Printf("signal received: %s, %s\n", connID, signalType)
    }),
)

// SSE handler that subscribes to updates storage
sseHandler := transport.NewStorageSseHandler(updates,
    transport.WithHistoryCount(10), // Send last 10 updates on connect
    transport.OnStorageConnected(func(connId string, active int) {
        log.Printf("connected: %s (%d active)\n", connId, active)
    }),
)
```

### Example Configurations

#### Channel Only (Default Behavior)

```go
signals := storage.NewChannelStorage()
updates := storage.NewChannelStorage()
dispatcher := core.NewStorageDispatcher(signals, updates)
```

#### Channel Signals, Redis Updates

```go
client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

signals := storage.NewChannelStorage()
updates := storage.NewRedisStorage(client, "sse:updates",
    storage.WithMaxLen(1000),
    storage.WithDecoder(func(id string, data []byte) (storage.Message, error) {
        return core.DecodeUpdate(id, data)
    }),
)
```

#### Both Redis

```go
client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

signals := storage.NewRedisStorage(client, "sse:signals",
    storage.WithConsumerGroup("dispatchers"),
    storage.WithMaxLen(10000),
    storage.WithDecoder(func(id string, data []byte) (storage.Message, error) {
        return core.DecodeSignal(id, data)
    }),
)

updates := storage.NewRedisStorage(client, "sse:updates",
    storage.WithMaxLen(1000),
    storage.WithDecoder(func(id string, data []byte) (storage.Message, error) {
        return core.DecodeUpdate(id, data)
    }),
)
```

## Running the Example

### With Docker Compose

```bash
cd cmd/example
docker-compose up
```

This starts:
- Redis on port 6379
- SSE server on port 8080 (configured to use Redis)

### Without Docker

```bash
# Start Redis locally
redis-server

# Run with Redis
go run ./cmd/example -redis localhost:6379

# Or run with in-memory channels
go run ./cmd/example
```

### Testing

```bash
# Connect SSE client
curl -N http://localhost:8080/live-updates

# Send a signal
curl -X POST http://localhost:8080/signals \
  -H "Content-Type: application/json" \
  -d '{"type":"message","data":{"text":"Hello!"}}'
```

## Redis Data Structure

Each storage instance uses one Redis Stream:

```
Key: {streamKey}
Type: Stream

Entry fields:
- id: string (message ID)
- data: string (JSON encoded message)
```

When `ConsumerGroup` is set:
- Uses Redis Stream consumer groups
- `XPENDING` for recovery on restart
- `XACK` marks message as processed

## Dependencies

- `github.com/redis/go-redis/v9` (only when using RedisStorage)
- `github.com/google/uuid` (for message IDs)
