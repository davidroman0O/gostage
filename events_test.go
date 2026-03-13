package gostage

import (
	"context"
	"sync"
	"testing"
	"time"
)

// testEventCollector collects events for test assertions.
type testEventCollector struct {
	mu     sync.Mutex
	events []EngineEvent
}

func (c *testEventCollector) OnEvent(event EngineEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, event)
}

func (c *testEventCollector) get() []EngineEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]EngineEvent, len(c.events))
	copy(out, c.events)
	return out
}

func (c *testEventCollector) byType(t EventType) []EngineEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []EngineEvent
	for _, e := range c.events {
		if e.Type == t {
			out = append(out, e)
		}
	}
	return out
}

func TestEventHandler_RunLifecycle(t *testing.T) {
	ResetTaskRegistry()
	Task("ev.task", func(ctx *Ctx) error {
		Set(ctx, "done", true)
		return nil
	})

	wf, err := NewWorkflow("ev-wf").Step("ev.task").Commit()
	if err != nil {
		t.Fatal(err)
	}

	collector := &testEventCollector{}
	engine, err := New(WithEventHandler(collector))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}

	events := collector.get()
	if len(events) < 2 {
		t.Fatalf("expected at least 2 events (created + completed), got %d", len(events))
	}

	created := collector.byType(EventRunCreated)
	if len(created) != 1 {
		t.Fatalf("expected 1 EventRunCreated, got %d", len(created))
	}
	if created[0].RunID != result.RunID {
		t.Fatalf("EventRunCreated RunID mismatch")
	}

	completed := collector.byType(EventRunCompleted)
	if len(completed) != 1 {
		t.Fatalf("expected 1 EventRunCompleted, got %d", len(completed))
	}
}

func TestEventHandler_Cancel(t *testing.T) {
	ResetTaskRegistry()
	Task("ev.slow", func(ctx *Ctx) error {
		time.Sleep(5 * time.Second)
		return nil
	})

	wf, err := NewWorkflow("ev-cancel").Step("ev.slow").Commit()
	if err != nil {
		t.Fatal(err)
	}

	collector := &testEventCollector{}
	engine, err := New(WithEventHandler(collector))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	runID, err := engine.Run(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Give it a moment to start
	time.Sleep(50 * time.Millisecond)

	if err := engine.Cancel(context.Background(), runID); err != nil {
		t.Fatal(err)
	}

	// Wait for the run to finish
	time.Sleep(100 * time.Millisecond)

	cancelled := collector.byType(EventRunCancelled)
	if len(cancelled) < 1 {
		t.Fatalf("expected at least 1 EventRunCancelled, got %d", len(cancelled))
	}
}

func TestEventHandler_PanicRecovery(t *testing.T) {
	ResetTaskRegistry()
	Task("ev.ok", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("ev-panic").Step("ev.ok").Commit()
	if err != nil {
		t.Fatal(err)
	}

	panicker := &panicEventHandler{}
	engine, err := New(WithEventHandler(panicker))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed despite panicking handler, got %s", result.Status)
	}
}

type panicEventHandler struct{}

func (h *panicEventHandler) OnEvent(_ EngineEvent) {
	panic("handler boom")
}
