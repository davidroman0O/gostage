package node

import (
	"context"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3/diagnostics"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/sasha-s/go-deadlock"
)

type recorderDiag struct {
	mu     deadlock.Mutex
	events []diagnostics.Event
}

func (r *recorderDiag) Write(evt diagnostics.Event) {
	r.mu.Lock()
	r.events = append(r.events, evt)
	r.mu.Unlock()
}

func TestTelemetryDispatcherFanOut(t *testing.T) {
	diag := &recorderDiag{}
	d := NewTelemetryDispatcher(context.Background(), diag)
	cs1 := telemetry.NewChannelSink(4)
	cs2 := telemetry.NewChannelSink(4)
	d.Register(cs1)
	d.Register(cs2)
	evt := telemetry.Event{Kind: "workflow_started", WorkflowID: "wf", Timestamp: time.Now()}
	d.Dispatch(evt)

	select {
	case received := <-cs1.C():
		if received.Kind != evt.Kind {
			t.Fatalf("sink1 expected %s, got %s", evt.Kind, received.Kind)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("sink1 did not receive event")
	}
	select {
	case received := <-cs2.C():
		if received.Kind != evt.Kind {
			t.Fatalf("sink2 expected %s, got %s", evt.Kind, received.Kind)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("sink2 did not receive event")
	}
	d.Close()
	if len(diag.events) != 0 {
		t.Fatalf("unexpected diagnostics: %#v", diag.events)
	}
}

func TestTelemetryDispatcherSinkPanicReported(t *testing.T) {
	diag := &recorderDiag{}
	d := NewTelemetryDispatcher(context.Background(), diag)
	d.Register(telemetry.SinkFunc(func(telemetry.Event) {}))
	d.Register(telemetry.SinkFunc(func(telemetry.Event) { panic("boom") }))
	d.Dispatch(telemetry.Event{Kind: "test"})
	time.Sleep(20 * time.Millisecond)
	d.Close()
	if len(diag.events) == 0 {
		t.Fatalf("expected diagnostic event on panic")
	}
	if diag.events[0].Component != "telemetry.sink" {
		t.Fatalf("unexpected diagnostic component: %#v", diag.events[0])
	}
}

func TestHealthDispatcher(t *testing.T) {
	h := NewHealthDispatcher()
	var events []HealthEvent
	h.Subscribe(func(evt HealthEvent) { events = append(events, evt) })
	input := HealthEvent{Pool: "local", Status: HealthHealthy, Timestamp: time.Now()}
	h.Publish(input)
	if len(events) != 1 || events[0].Pool != "local" {
		t.Fatalf("failed to publish health event: %#v", events)
	}
}
