package transport

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/miragepresent/go-sse-delivery/core"
	"github.com/miragepresent/go-sse-delivery/identity"
)

const defaultIngressTimeout = 20 * time.Millisecond

// SignalReceivedHandler is called when a signal is received
type SignalReceivedHandler func(connectionID string, signalType string)

type SignalReceiver struct {
	ingress               chan *core.Signal
	connectionIDResolver  identity.ConnectionIDResolver
	connectionIDGenerator identity.ConnectionIDGenerator
	onSignalReceived      SignalReceivedHandler
	onError               core.ErrorHandler
	ingressTimeout        time.Duration
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

// WithIngressTimeout sets the timeout for sending signals to the ingress channel
func WithIngressTimeout(timeout time.Duration) SignalReceiverOption {
	return func(sr *SignalReceiver) {
		sr.ingressTimeout = timeout
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

func NewSignalReceiver(ingress chan *core.Signal, options ...SignalReceiverOption) *SignalReceiver {
	sr := &SignalReceiver{
		ingress:               ingress,
		connectionIDGenerator: identity.DefaultConnectionIDGenerator,
		connectionIDResolver:  identity.DefaultConnectionIDResolver,
		ingressTimeout:        defaultIngressTimeout,
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
		signal.ConnectionID = connectionID

		if sr.onSignalReceived != nil {
			sr.onSignalReceived(signal.ConnectionID, signal.Type)
		}

		if !sr.sendSignal(&signal) {
			http.Error(w, "Server busy", http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(connectionID))
		return
	}

	signal.ConnectionID = connectionID

	if sr.onSignalReceived != nil {
		sr.onSignalReceived(signal.ConnectionID, signal.Type)
	}

	if !sr.sendSignal(&signal) {
		http.Error(w, "Server busy", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (sr *SignalReceiver) sendSignal(signal *core.Signal) bool {
	select {
	case sr.ingress <- signal:
		return true
	case <-time.After(sr.ingressTimeout):
		sr.reportError(core.ErrIngressTimeout)
		return false
	}
}

func (sr *SignalReceiver) reportError(err error) {
	if sr.onError != nil {
		sr.onError(err)
	}
}
