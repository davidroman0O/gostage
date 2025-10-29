package gostage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3/diagnostics"
	"github.com/davidroman0O/gostage/v3/process"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

type diagRecorder struct {
	mu     sync.Mutex
	events []diagnostics.Event
}

func (r *diagRecorder) Write(evt diagnostics.Event) {
	r.mu.Lock()
	r.events = append(r.events, evt)
	r.mu.Unlock()
}

func (r *diagRecorder) Events() []diagnostics.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]diagnostics.Event, len(r.events))
	copy(out, r.events)
	return out
}

func TestRemoteCoordinatorOnLogPublishesDiagnostics(t *testing.T) {
	rec := &diagRecorder{}
	rc := newRemoteCoordinatorBareForTest(rec, telemetry.NoopLogger{})

	entry := process.LogEntry{
		OccurredAt:  time.Now(),
		Level:       "info",
		Logger:      "child",
		Message:     "structured log",
		Attributes:  map[string]string{"key": "value"},
		Pool:        "remote",
		ChildNodeID: "child-1",
	}

	if err := rc.OnLog(context.Background(), nil, entry); err != nil {
		t.Fatalf("OnLog returned error: %v", err)
	}

	events := rec.Events()
	if len(events) == 0 {
		t.Fatalf("expected diagnostic event, got none")
	}
	logEvent := events[len(events)-1]
	if logEvent.Component != "remote.log.remote" {
		t.Fatalf("unexpected component %q", logEvent.Component)
	}
	if logEvent.Metadata["message"] != entry.Message {
		t.Fatalf("unexpected message metadata: %+v", logEvent.Metadata)
	}
	if logEvent.Metadata["structured"] != true {
		t.Fatalf("expected structured flag, got %+v", logEvent.Metadata)
	}
	if logEvent.Metadata["key"] != "value" {
		t.Fatalf("missing attributes: %+v", logEvent.Metadata)
	}
	if logEvent.Metadata["child_node_id"] != entry.ChildNodeID {
		t.Fatalf("missing child_node_id: %+v", logEvent.Metadata)
	}
}

func TestRemoteCoordinatorForwardChildLog(t *testing.T) {
	recorder := &diagRecorder{}
	rc := newRemoteCoordinatorBareForTest(recorder, telemetry.NoopLogger{})

	rc.ForwardChildLogForTest("pool-1", diagnostics.Event{
		Component: "spawner.process.stdout",
		Severity:  diagnostics.SeverityInfo,
		Metadata: map[string]any{
			"line": "child log line",
			"pid":  42,
		},
	})

	events := recorder.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 diagnostic event, got %d", len(events))
	}
	evt := events[0]
	if evt.Component != "remote.child.pool-1" {
		t.Fatalf("unexpected component %s", evt.Component)
	}
	if evt.Metadata["pool"] != "pool-1" {
		t.Fatalf("expected pool metadata, got %+v", evt.Metadata)
	}
	if evt.Metadata["line"] != "child log line" {
		t.Fatalf("expected log line metadata, got %+v", evt.Metadata)
	}
	if evt.Metadata["stream"] != "spawner.process.stdout" {
		t.Fatalf("expected stream metadata, got %+v", evt.Metadata)
	}
}
