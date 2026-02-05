package core

type Dispatcher struct {
	ingress  chan *Signal
	updates  chan *Update
	handlers map[string]Handler
	onError  ErrorHandler
}

// DispatcherOption configures a Dispatcher
type DispatcherOption func(d *Dispatcher)

// OnError sets the error handler for async errors
func OnError(handler ErrorHandler) DispatcherOption {
	return func(d *Dispatcher) {
		d.onError = handler
	}
}

func NewDispatcher(ingress chan *Signal, updates chan *Update, options ...DispatcherOption) *Dispatcher {
	d := &Dispatcher{
		ingress:  ingress,
		updates:  updates,
		handlers: map[string]Handler{},
	}

	for _, opt := range options {
		opt(d)
	}

	return d
}

func (d *Dispatcher) Register(signalType string, handler Handler) {
	if d.handlers == nil {
		d.handlers = map[string]Handler{}
	}

	d.handlers[signalType] = handler
}

func (d *Dispatcher) Start() {
	for signal := range d.ingress {
		h := d.getHandler(signal.Type)
		if h == nil {
			d.reportError(NoHandlerError(signal.Type))
			continue
		}

		upd, err := h.Handle(signal)
		if err != nil {
			d.reportError(err)
			continue
		}

		select {
		case d.updates <- upd:
		default:
			d.reportError(ErrChannelFull)
		}
	}
}

func (d *Dispatcher) getHandler(signalType string) Handler {
	if h, ok := d.handlers[signalType]; ok {
		return h
	}

	return nil
}

func (d *Dispatcher) reportError(err error) {
	if d.onError != nil {
		d.onError(err)
	}
}
