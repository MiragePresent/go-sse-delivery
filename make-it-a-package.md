# Package Structure Migration

Tasks to migrate from an app to a reusable package.

## Task 1: Move example app to cmd/

- [x] Move `main.go` to `cmd/example/main.go`
- [x] Move `static/index.html` to `cmd/example/static/index.html`
- [x] Update imports and file paths in main.go (added StaticPath to Config)

## Task 2: Refactor server to handlers

- [x] Rename `server/server.go` to `server/handlers.go`
- [x] Create a `Handlers` struct containing:
  - `clients` map for tracking connected SSE clients
  - `register` channel for new client registration
  - `unregister` channel for client disconnection
  - `ingress` channel for receiving events (replaces storage)
  - `updates` channel for broadcasting updates to clients
  - `mu` sync.RWMutex for thread-safe client access
- [x] Rename `sseHandler` to `DeliverEndpointHandler` (method of Handlers)
  - Registers client on connect
  - Listens to updates channel for its specific client
  - Flushes SSE messages to the response
  - Unregisters client on disconnect
- [x] Rename `postEventHandler` to `NewEventsHandler` (method of Handlers)
  - Parses incoming JSON event
  - Reads SSE-Client-ID header for sender identification
  - Pushes event to ingress channel
  - Returns 202 Accepted
- [x] Remove processor/storage/queue initialization from server package (moved to user code)
- [x] Add `NewHandlers()` constructor that initializes channels and maps
- [x] Add `Run()` method to handle client registration/unregistration and broadcast updates
- [x] Update `cmd/example/main.go` to:
  - Create Handlers instance
  - Create Processor externally
  - Wire up channels between them
  - Mount handlers on HTTP routes

## Task 3: External processor with Process method

- [x] Processor is created outside of server package (in main.go or user code)
- [x] Update `Processor` struct to accept:
  - `ingress` channel (receives events from NewEventsHandler)
  - `updates` channel (sends updates to DeliverEndpointHandler clients)
  - `handler` Handler interface (transforms Event → Update)
- [x] Add `Process()` method that runs a loop:
  - Listens to events from ingress channel
  - Calls `handler.Handle(event)` to transform Event to Update
  - Broadcasts Update to updates channel
  - Logs event received and update added
- [x] Remove EventsStorage and UpdatesQueue interfaces (replaced by direct channels)
- [x] `DeliverEndpointHandler` responsibilities:
  - Only listens to updates channel for messages targeting its client
  - Formats and flushes SSE messages to response
  - Does NOT process or transform events
- [x] `NewEventsHandler` responsibilities:
  - Only parses JSON and pushes Event to ingress channel
  - Does NOT call processor or handle updates
- [x] Update `cmd/example/main.go` to wire everything:
  ```go
  ingress := make(chan *messaging.Event, 100)
  updates := make(chan *messaging.Update, 100)

  handlers := server.NewHandlers(ingress, updates)
  processor := messaging.NewProcessor(ingress, updates, &messaging.EchoHandler{})

  go handlers.Run()
  go processor.Process()
  ```

## Target Architecture

```
[POST /events]
    │
    ▼
NewEventsHandler
    │ (parses JSON, pushes to ingress)
    ▼
ingress channel ◄──────────────────────┐
    │                                  │
    ▼                                  │
Processor.Process()                    │
    │ (calls Handler.Handle)           │
    ▼                                  │
updates channel                        │
    │                                  │
    ▼                                  │
Handlers.Run()                         │
    │ (broadcasts to all clients)      │
    ▼                                  │
DeliverEndpointHandler (per client)    │
    │ (listens, formats SSE, flushes)  │
    ▼                                  │
[SSE Response to Client] ──────────────┘
```

## Target Structure

```
go-sse-delivery/
├── cmd/
│   └── example/
│       ├── main.go
│       └── static/
│           └── index.html
├── messaging/
│   ├── event.go
│   ├── update.go
│   ├── handler.go
│   └── processor.go
└── server/
    └── handlers.go
```

Note: `storage.go` and `queue.go` will be removed (replaced by direct channels).
