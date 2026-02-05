# Go-SSE-Delivery

A Go package for simplified Server-Sent Events (SSE) message delivery.

## Purpose

This package provides an SSE endpoint for delivering real-time events from backend to connected clients. It supports:

- **Broadcast**: Send events to all connected clients
- **Authorized delivery**: Send events only to authenticated/authorized clients
- **Targeted delivery**: Send events to specific clients by unique identifier

## Use Cases

1. **Digital signage**: Kiosks displaying ads receive broadcast updates
2. **Internal displays**: Office kiosks with unique IDs receive both public and private events
3. **User-specific events**: Clients receive personalized events (e.g., order status) based on user context

## Architecture

The package should expose:
- An SSE endpoint handler for client connections
- Methods to broadcast messages to all clients
- Methods to send messages to specific clients by ID
- Support for client authentication/authorization
- Connection management (connect/disconnect handling)

## Technical Requirements

- **Go version**: 1.25.5+
- **Module**: `github.com/miragepresent/go-sse-delivery`
- **Dependencies**: Standard library only (`net/http` for HTTP handling)

## Event Sources

Events are delivered through a channel-based architecture:

- **Default source**: A Go channel that receives events from other HTTP endpoints
- The backend pushes events to the channel via a separate endpoint (e.g., POST /events)
- The SSE handler reads from the channel and distributes to connected clients
- This decouples event production from SSE delivery

```
[External Service] --POST--> [SignalReceiver] --ingress--> [Dispatcher] --updates--> [HttpHandler] --SSE--> [Clients]
```

## Roadmap

- [x] Create transport package with HttpHandler (SSE) and EventsReceiver (POST events)
- [x] Create core package (Signal, Update, Dispatcher, Handler)
- [x] Create identity package (ClientIdResolver for SSE-Client-ID header)
