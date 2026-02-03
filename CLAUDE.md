# Go-SSE-Delivery

A Go package for simplified Server-Sent Events (SSE) message delivery.

## Purpose

This package provides an SSE endpoint for delivering real-time events from backend to connected clients. It supports:

- **Broadcast**: Send events to all connected devices
- **Authorized delivery**: Send events only to authenticated/authorized devices
- **Targeted delivery**: Send events to specific devices by unique identifier

## Use Cases

1. **Digital signage**: Kiosks displaying ads receive broadcast updates
2. **Internal displays**: Office kiosks with unique IDs receive both public and private events
3. **User-specific events**: Devices receive personalized events (e.g., order status) based on user context

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
[External Service] --POST--> [Events Endpoint] --channel--> [SSE Handler] --SSE--> [Clients]
```

## Roadmap

- [x] Create server module with SSE endpoint and POST events endpoint
- [x] Add server configurations (port, endpoint paths for sending/receiving events)
- [ ] Create messaging module (message structure, channels, etc.)
