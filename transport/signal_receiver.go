package transport

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/miragepresent/go-sse-delivery/core"
	"github.com/miragepresent/go-sse-delivery/identity"
)

type SignalReceiver struct {
	ingress        chan *core.Signal
	clientResolver identity.ClientIdResolver
}

func (sr *SignalReceiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	clientID, err := sr.clientResolver(r)
	if err != nil || clientID == "" {
		log.Printf("cannot identify client ID. ignoring the signal. Error: %s", err)
		http.Error(w, "Unknown client ID. Disconnecting", http.StatusBadRequest)
		return
	}

	var signal core.Signal
	if err := json.NewDecoder(r.Body).Decode(&signal); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	signal.SenderID = clientID

	log.Printf("Signal received: senderId=%s, type=%s, data=%v", signal.SenderID, signal.Type, signal.Data)
	sr.ingress <- &signal
	w.WriteHeader(http.StatusAccepted)
}

func NewSignalReceiver(ingress chan *core.Signal) *SignalReceiver {
	return NewSignalReceiverWithClientResolver(ingress, identity.DefaultClientIdResolver)
}

func NewSignalReceiverWithClientResolver(ingress chan *core.Signal, resolver identity.ClientIdResolver) *SignalReceiver {
	return &SignalReceiver{
		ingress:        ingress,
		clientResolver: resolver,
	}
}
