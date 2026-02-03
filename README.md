# go-sse-delivery

A Go package for simplified Server-Sent Events (SSE) message delivery.

## Installation

```bash
go get github.com/miragepresent/go-sse-delivery
```

## Running the Example

```bash
# Run the example server
go run ./cmd/example

# Run with debug page enabled (available at http://localhost:8080/debug/)
go run ./cmd/example --debug

# Run on a custom port
go run ./cmd/example --port :3000
```

## Testing

```bash
# Connect to SSE endpoint
curl -N http://localhost:8080/live-updates

# Send an event
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{"senderId":"test","eventType":"message","data":"hello"}'
```
