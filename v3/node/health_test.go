package node

import (
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3/diagnostics"
)

type diagRecorder struct {
	events []diagnostics.Event
}

func (r *diagRecorder) Write(evt diagnostics.Event) {
	r.events = append(r.events, evt)
}

func TestDiagnosticsHealthWriter(t *testing.T) {
	rec := &diagRecorder{}
	writer := DiagnosticsHealthWriter{diag: rec}
	evt := HealthEvent{Pool: "local", Status: HealthUnavailable, Detail: "no workers", Timestamp: time.Now()}
	writer.Handle(evt)
	if len(rec.events) != 1 {
		t.Fatalf("expected diagnostic event, got %d", len(rec.events))
	}
	if rec.events[0].Component != "node.health" {
		t.Fatalf("unexpected diagnostic component: %#v", rec.events[0])
	}
}

func TestDiagnosticsHealthWriterIgnoresHealthy(t *testing.T) {
	rec := &diagRecorder{}
	writer := DiagnosticsHealthWriter{diag: rec}
	writer.Handle(HealthEvent{Pool: "local", Status: HealthHealthy})
	if len(rec.events) != 0 {
		t.Fatalf("expected no diagnostics for healthy event")
	}
}
