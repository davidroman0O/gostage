package gostage

import (
	"context"
	"sync"
	"testing"
)

// === Integration: Multiple features working together ===

func TestIntegration_TagsAndMutations(t *testing.T) {
	ResetTaskRegistry()

	Task("int.setup", func(ctx *Ctx) error {
		// Disable all optional steps by tag
		DisableByTag(ctx, "optional")
		// Insert a dynamic step
		InsertAfter(ctx, "int.dynamic")
		return nil
	})
	Task("int.dynamic", func(ctx *Ctx) error {
		Set(ctx, "dynamic_ran", true)
		return nil
	})
	Task("int.opt", func(ctx *Ctx) error {
		Set(ctx, "opt_ran", true)
		return nil
	})
	Task("int.finish", func(ctx *Ctx) error {
		Set(ctx, "finish_ran", true)
		return nil
	})

	wf, err := NewWorkflow("int-combo").
		Step("int.setup").
		Step("int.opt", WithStepTags("optional")).
		Step("int.finish").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (err: %v)", result.Status, result.Error)
	}

	if result.Store["dynamic_ran"] != true {
		t.Fatal("dynamic step should have run")
	}
	if result.Store["opt_ran"] == true {
		t.Fatal("optional step should have been disabled")
	}
	if result.Store["finish_ran"] != true {
		t.Fatal("finish step should have run")
	}
}

func TestIntegration_MiddlewareAndIPC(t *testing.T) {
	ResetTaskRegistry()

	var msgs []string
	var mu sync.Mutex

	Task("int.ipc", func(ctx *Ctx) error {
		Send(ctx, "status", Params{"msg": "running"})
		return nil
	})

	wf, err := NewWorkflow("int-mw-ipc").Step("int.ipc").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New(
		WithTaskMiddleware(func(tctx *Ctx, taskName string, next func() error) error {
			mu.Lock()
			msgs = append(msgs, "mw:"+taskName)
			mu.Unlock()
			return next()
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	engine.OnMessage("status", func(msgType string, payload map[string]any) {
		mu.Lock()
		msgs = append(msgs, "ipc:"+msgType)
		mu.Unlock()
	})

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}

	// Should have both middleware and IPC entries
	mu.Lock()
	defer mu.Unlock()
	hasMW, hasIPC := false, false
	for _, m := range msgs {
		if m == "mw:int.ipc" {
			hasMW = true
		}
		if m == "ipc:status" {
			hasIPC = true
		}
	}
	if !hasMW {
		t.Fatal("task middleware should have been called")
	}
	if !hasIPC {
		t.Fatal("IPC handler should have been called")
	}
}
