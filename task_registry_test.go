package gostage

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestTask_Register(t *testing.T) {
	ResetTaskRegistry()

	Task("tr.test", func(ctx *Ctx) error { return nil })

	td := lookupTask("tr.test")
	if td == nil {
		t.Fatal("expected task to be registered")
	}
	if td.name != "tr.test" {
		t.Fatalf("expected name 'tr.test', got %s", td.name)
	}
}

func TestTask_WithRetry(t *testing.T) {
	ResetTaskRegistry()

	Task("tr.retry", func(ctx *Ctx) error { return nil },
		WithRetry(3),
		WithRetryDelay(500*time.Millisecond),
	)

	td := lookupTask("tr.retry")
	if td.retries != 3 {
		t.Fatalf("expected 3 retries, got %d", td.retries)
	}
	if td.retryDelay != 500*time.Millisecond {
		t.Fatalf("expected 500ms delay, got %s", td.retryDelay)
	}
}

func TestTask_DuplicatePanics(t *testing.T) {
	ResetTaskRegistry()

	Task("tr.dup", func(ctx *Ctx) error { return nil })

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for duplicate task")
		}
	}()

	Task("tr.dup", func(ctx *Ctx) error { return nil })
}

func TestTask_LookupNotFound(t *testing.T) {
	ResetTaskRegistry()

	td := lookupTask("nonexistent")
	if td != nil {
		t.Fatal("expected nil for unregistered task")
	}
}

func TestTask_ResetRegistry(t *testing.T) {
	ResetTaskRegistry()

	Task("tr.temp", func(ctx *Ctx) error { return nil })
	if lookupTask("tr.temp") == nil {
		t.Fatal("expected task before reset")
	}

	ResetTaskRegistry()
	if lookupTask("tr.temp") != nil {
		t.Fatal("expected nil after reset")
	}
}

// TestEngine_RegistryIsolation verifies two engines with separate registries
// can run concurrently without cross-contamination.
func TestEngine_RegistryIsolation(t *testing.T) {
	regA := NewRegistry()
	regA.RegisterTask("iso.task", func(ctx *Ctx) error {
		Set(ctx, "source", "engine-A")
		return nil
	})

	regB := NewRegistry()
	regB.RegisterTask("iso.task", func(ctx *Ctx) error {
		Set(ctx, "source", "engine-B")
		return nil
	})

	wfA, err := NewWorkflow("wf-a").Step("iso.task").Commit()
	if err != nil {
		t.Fatal(err)
	}
	wfB, err := NewWorkflow("wf-b").Step("iso.task").Commit()
	if err != nil {
		t.Fatal(err)
	}

	engineA, err := New(WithRegistry(regA))
	if err != nil {
		t.Fatal(err)
	}
	defer engineA.Close()

	engineB, err := New(WithRegistry(regB))
	if err != nil {
		t.Fatal(err)
	}
	defer engineB.Close()

	// Run concurrently
	var wg sync.WaitGroup
	var resultA, resultB *Result
	var errA, errB error

	wg.Add(2)
	go func() {
		defer wg.Done()
		resultA, errA = engineA.RunSync(context.Background(), wfA, nil)
	}()
	go func() {
		defer wg.Done()
		resultB, errB = engineB.RunSync(context.Background(), wfB, nil)
	}()
	wg.Wait()

	if errA != nil {
		t.Fatalf("engine A: %v", errA)
	}
	if errB != nil {
		t.Fatalf("engine B: %v", errB)
	}

	sourceA, _ := ResultGet[string](resultA, "source")
	sourceB, _ := ResultGet[string](resultB, "source")

	if sourceA != "engine-A" {
		t.Fatalf("engine A produced %q, want %q", sourceA, "engine-A")
	}
	if sourceB != "engine-B" {
		t.Fatalf("engine B produced %q, want %q", sourceB, "engine-B")
	}
}
