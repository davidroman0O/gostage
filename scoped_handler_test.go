package gostage

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestOnMessageForRun_OnlyReceivesOwnMessages(t *testing.T) {
	ResetTaskRegistry()

	var mu sync.Mutex
	var scopedMsgs []string
	var globalMsgs []string

	Task("sh.sender", func(ctx *Ctx) error {
		Send(ctx, "progress", map[string]any{"step": "done"})
		return nil
	})

	wf, err := NewWorkflow("sh-wf").Step("sh.sender").Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// Global handler — receives all messages
	engine.OnMessage("progress", func(msgType string, payload map[string]any) {
		mu.Lock()
		globalMsgs = append(globalMsgs, msgType)
		mu.Unlock()
	})

	// Run the workflow
	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Register a scoped handler for a DIFFERENT run — should NOT receive
	engine.OnMessageForRun("progress", RunID("other-run"), func(msgType string, payload map[string]any) {
		mu.Lock()
		scopedMsgs = append(scopedMsgs, msgType)
		mu.Unlock()
	})

	// Run again — the scoped handler is for "other-run", not this run
	result2, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed || result2.Status != Completed {
		t.Fatal("workflows should complete")
	}

	mu.Lock()
	defer mu.Unlock()

	if len(globalMsgs) < 2 {
		t.Fatalf("global handler should receive all messages, got %d", len(globalMsgs))
	}
	if len(scopedMsgs) != 0 {
		t.Fatalf("scoped handler for other-run should receive 0 messages, got %d", len(scopedMsgs))
	}
}

func TestOnMessageForRun_ReceivesMatchingRun(t *testing.T) {
	ResetTaskRegistry()

	var received []string
	var mu sync.Mutex

	Task("sh2.task", func(ctx *Ctx) error {
		Send(ctx, "status", map[string]any{"ok": true})
		return nil
	})

	wf, err := NewWorkflow("sh2-wf").Step("sh2.task").Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// Use Run (async) so we can get the ID and register the scoped handler
	runID, err := engine.Run(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}

	engine.OnMessageForRun("status", runID, func(msgType string, payload map[string]any) {
		mu.Lock()
		received = append(received, msgType)
		mu.Unlock()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := engine.Wait(ctx, runID)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 {
		t.Fatalf("scoped handler should receive 1 message, got %d", len(received))
	}
}
