package gostage

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

// === IPC Message Handlers ===

func TestOnMessage(t *testing.T) {
	ResetTaskRegistry()

	var received sync.Map

	Task("ipc.sender", func(ctx *Ctx) error {
		return Send(ctx, "progress", Params{"pct": 50})
	})

	wf, err := NewWorkflow("on-message").Step("ipc.sender").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	engine.OnMessage("progress", func(msgType string, payload map[string]any) {
		received.Store("type", msgType)
		received.Store("pct", payload["pct"])
	})

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}

	msgType, ok := received.Load("type")
	if !ok || msgType != "progress" {
		t.Fatalf("expected progress message, got %v", msgType)
	}

	pct, _ := received.Load("pct")
	if pct != 50 {
		t.Fatalf("expected pct 50, got %v (%T)", pct, pct)
	}
}

func TestOnMessageWildcard(t *testing.T) {
	ResetTaskRegistry()

	var msgCount int32

	Task("ipc.multi", func(ctx *Ctx) error {
		Send(ctx, "type_a", Params{"x": 1})
		Send(ctx, "type_b", Params{"x": 2})
		return nil
	})

	wf, err := NewWorkflow("wildcard-handler").Step("ipc.multi").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	engine.OnMessage("*", func(msgType string, payload map[string]any) {
		atomic.AddInt32(&msgCount, 1)
	})

	engine.RunSync(context.Background(), wf, nil)

	if atomic.LoadInt32(&msgCount) != 2 {
		t.Fatalf("expected 2 wildcard messages, got %d", msgCount)
	}
}

// === OnMessage / OffMessage with HandlerID (Issue 5) ===

func TestOnMessageReturnsID(t *testing.T) {
	ResetTaskRegistry()

	Task("hid.sender", func(ctx *Ctx) error {
		return Send(ctx, "evt", Params{"x": 1})
	})

	wf, err := NewWorkflow("handler-id").Step("hid.sender").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	var aFired, bFired int32

	idA := engine.OnMessage("evt", func(msgType string, payload map[string]any) {
		atomic.AddInt32(&aFired, 1)
	})
	idB := engine.OnMessage("evt", func(msgType string, payload map[string]any) {
		atomic.AddInt32(&bFired, 1)
	})

	if idA == 0 || idB == 0 {
		t.Fatal("OnMessage must return non-zero HandlerID")
	}
	if idA == idB {
		t.Fatal("each OnMessage call must return a distinct HandlerID")
	}

	// Deregister handler A.
	engine.OffMessage(idA)

	engine.RunSync(context.Background(), wf, nil)

	if atomic.LoadInt32(&aFired) != 0 {
		t.Fatal("deregistered handler A must not fire")
	}
	if atomic.LoadInt32(&bFired) != 1 {
		t.Fatalf("handler B must fire once, fired %d times", atomic.LoadInt32(&bFired))
	}
}

func TestOffMessage_Anonymous(t *testing.T) {
	ResetTaskRegistry()

	Task("anon.sender", func(ctx *Ctx) error {
		return Send(ctx, "evt2", Params{"v": 2})
	})

	wf, err := NewWorkflow("offmsg-anon").Step("anon.sender").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	var fired int32

	// Register an anonymous inline handler and deregister it immediately.
	id := engine.OnMessage("evt2", func(msgType string, payload map[string]any) {
		atomic.AddInt32(&fired, 1)
	})
	engine.OffMessage(id)

	engine.RunSync(context.Background(), wf, nil)

	if atomic.LoadInt32(&fired) != 0 {
		t.Fatal("deregistered inline handler must not fire")
	}
}

// TestOnMessageNilHandler verifies that registering a nil handler is silently ignored.
func TestOnMessageNilHandler(t *testing.T) {
	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// Should not panic
	engine.OnMessage("test", nil)

	// Verify no handler was registered by checking that dispatching does nothing
	// (if a nil handler were registered, dispatching would panic)
	engine.dispatchMessage("test", map[string]any{"key": "value"})
}
