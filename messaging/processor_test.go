package messaging

import (
	"testing"
	"time"
)

func TestNewProcessor(t *testing.T) {
	ingress := make(chan *Event)
	updates := make(chan *Update)
	handler := &EchoHandler{}

	processor := NewProcessor(ingress, updates, handler)

	if processor.ingress != ingress {
		t.Error("ingress channel not set correctly")
	}
	if processor.updates != updates {
		t.Error("updates channel not set correctly")
	}
	if processor.handler != handler {
		t.Error("handler not set correctly")
	}
}

func TestProcessor_Process(t *testing.T) {
	ingress := make(chan *Event, 1)
	updates := make(chan *Update, 1)
	handler := &EchoHandler{}

	processor := NewProcessor(ingress, updates, handler)

	go processor.Process()

	event := &Event{
		SenderID:  "test-client",
		EventType: "test",
		Data:      "test-data",
	}
	ingress <- event

	select {
	case update := <-updates:
		if update.Data != "test-data" {
			t.Errorf("got %v, want %v", update.Data, "test-data")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for update")
	}

	close(ingress)
}

func TestProcessor_Process_NilUpdate(t *testing.T) {
	ingress := make(chan *Event, 1)
	updates := make(chan *Update, 1)
	handler := &mockHandler{returnNil: true}

	processor := NewProcessor(ingress, updates, handler)

	go processor.Process()

	event := &Event{
		SenderID:  "test-client",
		EventType: "test",
		Data:      "test-data",
	}
	ingress <- event

	select {
	case <-updates:
		t.Fatal("should not receive update when handler returns nil")
	case <-time.After(50 * time.Millisecond):
		// Expected: no update received
	}

	if !handler.wasCalled() {
		t.Error("handler was not called")
	}

	close(ingress)
}

func TestProcessor_Process_MultipleEvents(t *testing.T) {
	ingress := make(chan *Event, 3)
	updates := make(chan *Update, 3)
	handler := &EchoHandler{}

	processor := NewProcessor(ingress, updates, handler)

	go processor.Process()

	events := []*Event{
		{SenderID: "c1", EventType: "t1", Data: "data1"},
		{SenderID: "c2", EventType: "t2", Data: "data2"},
		{SenderID: "c3", EventType: "t3", Data: "data3"},
	}

	for _, e := range events {
		ingress <- e
	}

	for i, expected := range events {
		select {
		case update := <-updates:
			if update.Data != expected.Data {
				t.Errorf("event %d: got %v, want %v", i, update.Data, expected.Data)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("timeout waiting for update %d", i)
		}
	}

	close(ingress)
}

func TestProcessor_Process_ChannelClose(t *testing.T) {
	ingress := make(chan *Event)
	updates := make(chan *Update, 1)
	handler := &EchoHandler{}

	processor := NewProcessor(ingress, updates, handler)

	done := make(chan struct{})
	go func() {
		processor.Process()
		close(done)
	}()

	close(ingress)

	select {
	case <-done:
		// Process exited as expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Process did not exit after ingress channel closed")
	}
}
