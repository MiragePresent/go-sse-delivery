package core

import (
	"errors"
	"fmt"
)

// ErrorHandler is called when async errors occur
type ErrorHandler func(err error)

var (
	// ErrNoHandler is reported when a signal arrives but no handler is registered for its type.
	// Triggered by: Dispatcher.Start() when processing signals from the ingress channel.
	// Context: Use NoHandlerError(signalType) to include the signal type.
	ErrNoHandler = errors.New("no handler registered for signal type")

	// ErrConnectionNotFound is reported when sending an update to a connection that doesn't exist.
	// Triggered by: HttpHandler.sendToConnections() when a target connection ID is not in the pool.
	// Context: Use ConnectionNotFoundError(connID) to include the connection ID.
	ErrConnectionNotFound = errors.New("connection not found")

	// ErrBufferFull is reported when a connection's send buffer is full and the update is dropped.
	// Triggered by: HttpHandler.broadcastUpdate() and HttpHandler.sendToConnections()
	// when the connection's send channel cannot accept more messages.
	// Context: Use BufferFullError(connID) to include the connection ID.
	ErrBufferFull = errors.New("connection buffer full")

	// ErrChannelFull is reported when the updates channel is full and the update is dropped.
	// Triggered by: Dispatcher.Start() when the updates channel cannot accept the handler's result.
	ErrChannelFull = errors.New("channel full, update dropped")

	// ErrSerializationFailed is reported when update data cannot be serialized to JSON.
	// Triggered by: HttpHandler.broadcastUpdate() and HttpHandler.sendToConnections()
	// when json.Marshal fails on the update data.
	// Context: Use SerializationError(err) to include the underlying JSON error.
	ErrSerializationFailed = errors.New("serialization failed")

	// ErrNoConnections is reported when a targeted update has an empty connections list.
	// Triggered by: HttpHandler.sendToConnections() when upd.Connections is empty.
	ErrNoConnections = errors.New("no connection IDs in update")

	// ErrUnknownDeliveryMode is reported when an update has an unrecognized delivery mode.
	// Triggered by: HttpHandler.manageConnections() when upd.Mode is not Broadcast or Target.
	ErrUnknownDeliveryMode = errors.New("unknown delivery mode")

	// ErrIngressTimeout is reported when the ingress channel doesn't accept a signal within the timeout.
	// Triggered by: SignalReceiver.sendSignal() when the ingress channel is full.
	// The HTTP request receives a 503 Service Unavailable response.
	ErrIngressTimeout = errors.New("ingress channel timeout")
)

// NoHandlerError wraps ErrNoHandler with the signal type context.
func NoHandlerError(signalType string) error {
	return fmt.Errorf("%w: %s", ErrNoHandler, signalType)
}

// ConnectionNotFoundError wraps ErrConnectionNotFound with the connection ID.
func ConnectionNotFoundError(connID string) error {
	return fmt.Errorf("%w: %s", ErrConnectionNotFound, connID)
}

// BufferFullError wraps ErrBufferFull with the connection ID.
func BufferFullError(connID string) error {
	return fmt.Errorf("%w: %s", ErrBufferFull, connID)
}

// SerializationError wraps ErrSerializationFailed with the underlying error.
func SerializationError(err error) error {
	return errors.Join(ErrSerializationFailed, err)
}
