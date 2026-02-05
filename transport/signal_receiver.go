package transport

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/miragepresent/go-sse-delivery/core"
	"github.com/miragepresent/go-sse-delivery/identity"
	"github.com/miragepresent/go-sse-delivery/storage"
)

// SignalReceivedHandler is called when a signal is received
type SignalReceivedHandler func(connectionID string, signalType string)

type SignalReceiver struct {
	signals               storage.Storage
	connectionIDResolver  identity.ConnectionIDResolver
	connectionIDGenerator identity.ConnectionIDGenerator
	onSignalReceived      SignalReceivedHandler
	onError               core.ErrorHandler
	pushTimeout           time.Duration
}

// SignalReceiverOption configures a SignalReceiver
type SignalReceiverOption func(sr *SignalReceiver)

// OnSignalReceived sets the callback for received signals
func OnSignalReceived(handler SignalReceivedHandler) SignalReceiverOption {
	return func(sr *SignalReceiver) {
		sr.onSignalReceived = handler
	}
}

// OnSignalError sets the error handler for SignalReceiver
func OnSignalError(handler core.ErrorHandler) SignalReceiverOption {
	return func(sr *SignalReceiver) {
		sr.onError = handler
	}
}

// WithPushTimeout sets the timeout for pushing signals to storage
func WithPushTimeout(timeout time.Duration) SignalReceiverOption {
	return func(sr *SignalReceiver) {
		sr.pushTimeout = timeout
	}
}

// WithSignalConnectionIDGenerator sets a custom connection ID generator
func WithSignalConnectionIDGenerator(generator identity.ConnectionIDGenerator) SignalReceiverOption {
	return func(sr *SignalReceiver) {
		sr.connectionIDGenerator = generator
	}
}

// WithSignalConnectionIDResolver sets a custom connection ID resolver
func WithSignalConnectionIDResolver(resolver identity.ConnectionIDResolver) SignalReceiverOption {
	return func(sr *SignalReceiver) {
		sr.connectionIDResolver = resolver
	}
}

func NewSignalReceiver(signals storage.Storage, options ...SignalReceiverOption) *SignalReceiver {
	sr := &SignalReceiver{
		signals:               signals,
		connectionIDGenerator: identity.DefaultConnectionIDGenerator,
		connectionIDResolver:  identity.DefaultConnectionIDResolver,
		pushTimeout:           5 * time.Second,
	}

	for _, opt := range options {
		opt(sr)
	}

	return sr
}

func (sr *SignalReceiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var signal core.Signal
	if err := json.NewDecoder(r.Body).Decode(&signal); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	connectionID := sr.connectionIDResolver(r)
	if connectionID == "" {
		connectionID = sr.connectionIDGenerator()
	}
	signal.ConnectionID = connectionID

	if sr.onSignalReceived != nil {
		sr.onSignalReceived(signal.ConnectionID, signal.Type)
	}

	ctx, cancel := context.WithTimeout(r.Context(), sr.pushTimeout)
	defer cancel()

	if err := sr.signals.Push(ctx, &signal); err != nil {
		sr.reportError(err)
		http.Error(w, "Server busy", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(connectionID))
}

func (sr *SignalReceiver) reportError(err error) {
	if sr.onError != nil {
		sr.onError(err)
	}
}
