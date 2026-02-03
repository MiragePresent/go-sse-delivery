# go-sse-delivery

[![Tests](https://github.com/miragepresent/go-sse-delivery/actions/workflows/test.yml/badge.svg)](https://github.com/miragepresent/go-sse-delivery/actions/workflows/test.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/miragepresent/go-sse-delivery.svg)](https://pkg.go.dev/github.com/miragepresent/go-sse-delivery)
[![Version](https://img.shields.io/badge/version-v0.0.1--alpha-blue)](https://github.com/miragepresent/go-sse-delivery/releases/tag/v0.0.1-alpha)

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
