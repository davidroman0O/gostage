package node

import (
	"context"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3/diagnostics"
	"github.com/davidroman0O/gostage/v3/internal/locks"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

type recorderDiag struct {
	mu     locks.Mutex
	events []diagnostics.Event
}

func (r *recorderDiag) Write(evt diagnostics.Event) {
	r.mu.Lock()
	r.events = append(r.events, evt)
	r.mu.Unlock()
}

func newStoppedDispatcher(cfg TelemetryDispatcherConfig, diag DiagnosticsWriter) *TelemetryDispatcher {
	d := NewTelemetryDispatcher(context.Background(), diag, cfg)
	d.cancel()
	d.wg.Wait()
	d.ctx = context.Background()
	d.cancel = func() {}
	return d
}

func TestTelemetryDispatcherFanOut(t *testing.T) {
	diag := &recorderDiag{}
	d := NewTelemetryDispatcher(context.Background(), diag, TelemetryDispatcherConfig{})
	cs1 := telemetry.NewChannelSink(4)
	cs2 := telemetry.NewChannelSink(4)
	cancel1 := d.Register(cs1)
	cancel2 := d.Register(cs2)
	evt := telemetry.Event{Kind: telemetry.EventWorkflowStarted, WorkflowID: "wf", Timestamp: time.Now()}
	if err := d.Dispatch(evt); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

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
	cancel1()
	cancel2()
	d.Close()
	if len(diag.events) != 0 {
		t.Fatalf("unexpected diagnostics: %#v", diag.events)
	}
}

func TestTelemetryDispatcherSinkPanicReported(t *testing.T) {
	diag := &recorderDiag{}
	d := NewTelemetryDispatcher(context.Background(), diag, TelemetryDispatcherConfig{})
	_ = d.Register(telemetry.SinkFunc(func(telemetry.Event) {}))
	_ = d.Register(telemetry.SinkFunc(func(telemetry.Event) { panic("boom") }))
	if err := d.Dispatch(telemetry.Event{Kind: telemetry.EventKind("test")}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
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
	cancel := h.Subscribe(func(evt HealthEvent) { events = append(events, evt) })
	input := HealthEvent{Pool: "local", Status: HealthHealthy, Timestamp: time.Now()}
	h.Publish(input)
	if len(events) != 1 || events[0].Pool != "local" {
		t.Fatalf("failed to publish health event: %#v", events)
	}
	cancel()
	h.Publish(HealthEvent{Pool: "local", Status: HealthDegraded})
	if len(events) != 1 {
		t.Fatalf("expected no events after cancel")
	}
}

func TestTelemetryDispatcherCancelStopsEvents(t *testing.T) {
	d := NewTelemetryDispatcher(context.Background(), nil, TelemetryDispatcherConfig{})
	cs := telemetry.NewChannelSink(1)
	cancel := d.Register(cs)
	cancel()
	if err := d.Dispatch(telemetry.Event{Kind: telemetry.EventKind("late")}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	select {
	case <-cs.C():
		t.Fatalf("expected no events after cancel")
	case <-time.After(50 * time.Millisecond):
	}
	d.Close()
}

func TestTelemetryDispatcherDropOldestEvicts(t *testing.T) {
	diag := &recorderDiag{}
	cfg := TelemetryDispatcherConfig{BufferSize: 1, OverflowStrategy: OverflowStrategyDropOldest}
	d := newStoppedDispatcher(cfg, diag)
	defer d.Close()

	oldEvt := telemetry.Event{WorkflowID: "old"}
	newEvt := telemetry.Event{WorkflowID: "new"}

	d.events <- oldEvt
	if err := d.Dispatch(newEvt); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	select {
	case evt := <-d.events:
		if evt.WorkflowID != "new" {
			t.Fatalf("expected new event, got %q", evt.WorkflowID)
		}
	default:
		t.Fatalf("expected event in buffer")
	}

	if len(diag.events) == 0 {
		t.Fatalf("expected overflow diagnostic")
	}
	if reason, _ := diag.events[0].Metadata["reason"].(string); reason != "drop_oldest" {
		t.Fatalf("expected reason drop_oldest, got %q", reason)
	}
}

func TestTelemetryDispatcherFailFast(t *testing.T) {
	cfg := TelemetryDispatcherConfig{BufferSize: 1, OverflowStrategy: OverflowStrategyFailFast}
	d := newStoppedDispatcher(cfg, nil)
	defer d.Close()

	d.events <- telemetry.Event{WorkflowID: "occupied"}
	if err := d.Dispatch(telemetry.Event{WorkflowID: "over"}); err == nil {
		t.Fatalf("expected fail-fast error")
	}
	if stats := d.Stats(); stats.Dropped != 1 {
		t.Fatalf("expected dropped count 1, got %+v", stats)
	}
}

func TestTelemetryDispatcherBlockTimeout(t *testing.T) {
	cfg := TelemetryDispatcherConfig{BufferSize: 1, OverflowStrategy: OverflowStrategyBlock, OverflowTimeout: 20 * time.Millisecond}
	d := newStoppedDispatcher(cfg, nil)
	defer d.Close()

	d.events <- telemetry.Event{WorkflowID: "occupied"}
	start := time.Now()
	err := d.Dispatch(telemetry.Event{WorkflowID: "over"})
	if err == nil || err.Error() != "telemetry dispatcher buffer full (timeout)" {
		t.Fatalf("expected timeout error, got %v", err)
	}
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Fatalf("expected dispatch to block, elapsed %s", elapsed)
	}
}

func TestTelemetryDispatcherSinkOverflowReports(t *testing.T) {
	diag := &recorderDiag{}
	cfg := TelemetryDispatcherConfig{BufferSize: 2, SinkBuffer: 0}
	d := NewTelemetryDispatcher(context.Background(), diag, cfg)
	d.cfg.SinkBuffer = 0
	defer d.Close()

	block := make(chan struct{})
	cancel := d.Register(telemetry.SinkFunc(func(telemetry.Event) {
		<-block
	}))
	defer cancel()

	if err := d.Dispatch(telemetry.Event{WorkflowID: "first"}); err != nil {
		t.Fatalf("dispatch first: %v", err)
	}
	if err := d.Dispatch(telemetry.Event{WorkflowID: "second"}); err != nil {
		t.Fatalf("dispatch second: %v", err)
	}

	time.Sleep(20 * time.Millisecond)
	close(block)

	if len(diag.events) == 0 {
		t.Fatalf("expected sink overflow diagnostic")
	}
	found := false
	for _, evt := range diag.events {
		if evt.Component == "telemetry.sink" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected telemetry.sink diagnostic, got %#v", diag.events)
	}
}
