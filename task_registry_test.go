package gostage

import (
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
