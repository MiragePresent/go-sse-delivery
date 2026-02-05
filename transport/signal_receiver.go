package transport

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/miragepresent/go-sse-delivery/core"
	"github.com/miragepresent/go-sse-delivery/identity"
)

type SignalReceiver struct {
	ingress               chan *core.Signal
	connectionIDResolver  identity.ConnectionIDResolver
	connectionIDGenerator identity.ConnectionIDGenerator
}

func NewSignalReceiver(ingress chan *core.Signal) *SignalReceiver {
	return NewCustomSignalReceiver(
		ingress,
		identity.DefaultConnectionIDGenerator,
		identity.DefaultConnectionIDResolver,
	)
}

func NewCustomSignalReceiver(
	ingress chan *core.Signal,
	generator identity.ConnectionIDGenerator,
	resolver identity.ConnectionIDResolver,
) *SignalReceiver {
	return &SignalReceiver{
		ingress:               ingress,
		connectionIDGenerator: generator,
		connectionIDResolver:  resolver,
	}
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

		log.Printf("Signal received (new connection): connectionId=%s, type=%s, data=%v", signal.ConnectionID, signal.Type, signal.Data)
		sr.ingress <- &signal

		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(connectionID))
		return
	}

	signal.ConnectionID = connectionID

	log.Printf("Signal received: connectionId=%s, type=%s, data=%v", signal.ConnectionID, signal.Type, signal.Data)
	sr.ingress <- &signal
	w.WriteHeader(http.StatusAccepted)
}
