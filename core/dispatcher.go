package core

import (
	"context"

	"github.com/miragepresent/go-sse-delivery/storage"
)

type Dispatcher struct {
	signals  storage.Storage
	updates  storage.Storage
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

func NewDispatcher(signals, updates storage.Storage, options ...DispatcherOption) *Dispatcher {
	d := &Dispatcher{
		signals:  signals,
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

func (d *Dispatcher) Start(ctx context.Context) error {
	// Restore unprocessed signals first
	restored, err := d.signals.Restore(ctx)
	if err != nil {
		d.reportError(err)
	} else {
		for _, msg := range restored {
			signal, ok := msg.(*Signal)
			if !ok {
				continue
			}
			d.processSignal(ctx, signal)
		}
	}

	// Subscribe to new signals
	sub, err := d.signals.Subscribe(ctx, storage.SubscribeOptions{
		ConsumerID: "dispatcher",
	})
	if err != nil {
		return err
	}
	defer sub.Close()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-sub.Messages():
			if !ok {
				return nil
			}
			signal, ok := msg.(*Signal)
			if !ok {
				continue
			}
			d.processSignal(ctx, signal)
			sub.Ack(msg.ID())
		}
	}
}

func (d *Dispatcher) processSignal(ctx context.Context, signal *Signal) {
	h := d.getHandler(signal.Type)
	if h == nil {
		d.reportError(NoHandlerError(signal.Type))
		return
	}

	upd, err := h.Handle(signal)
	if err != nil {
		d.reportError(err)
		return
	}

	if err := d.updates.Push(ctx, upd); err != nil {
		d.reportError(err)
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
