package gostage

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestFixedDelay(t *testing.T) {
	s := FixedDelay(500 * time.Millisecond)
	for attempt := 0; attempt < 5; attempt++ {
		d := s.Delay(attempt)
		if d != 500*time.Millisecond {
			t.Fatalf("attempt %d: expected 500ms, got %s", attempt, d)
		}
	}
}

func TestExponentialBackoff(t *testing.T) {
	s := ExponentialBackoff(100*time.Millisecond, 5*time.Second)

	expected := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
		3200 * time.Millisecond,
		5 * time.Second,
		5 * time.Second,
	}

	for i, want := range expected {
		got := s.Delay(i)
		if got != want {
			t.Fatalf("attempt %d: expected %s, got %s", i, want, got)
		}
	}
}

func TestExponentialBackoff_ZeroMax(t *testing.T) {
	s := ExponentialBackoff(100*time.Millisecond, 0)
	d := s.Delay(10)
	if d != 100*time.Millisecond*1024 {
		t.Fatalf("expected %s, got %s", 100*time.Millisecond*1024, d)
	}
}

func TestExponentialBackoffWithJitter(t *testing.T) {
	s := ExponentialBackoffWithJitter(100*time.Millisecond, 5*time.Second)

	for attempt := 0; attempt < 20; attempt++ {
		d := s.Delay(attempt)
		if d < 0 {
			t.Fatalf("attempt %d: negative delay %s", attempt, d)
		}
		if d > 5*time.Second {
			t.Fatalf("attempt %d: exceeded max %s, got %s", attempt, 5*time.Second, d)
		}
	}

	delays := make(map[time.Duration]bool)
	for i := 0; i < 20; i++ {
		delays[s.Delay(3)] = true
	}
	if len(delays) < 2 {
		t.Fatal("jitter produced no variation across 20 calls")
	}
}

func TestWithRetryStrategy(t *testing.T) {
	ResetTaskRegistry()
	strategy := ExponentialBackoff(100*time.Millisecond, 5*time.Second)
	Task("rs.task", func(ctx *Ctx) error { return nil },
		WithRetry(3),
		WithRetryStrategy(strategy),
	)
	td := defaultRegistry.lookupTask("rs.task")
	if td == nil {
		t.Fatal("task not registered")
	}
	if td.retryStrategy == nil {
		t.Fatal("expected retry strategy to be set")
	}
}

func TestWithRetryDelay_CreatesFixedStrategy(t *testing.T) {
	ResetTaskRegistry()
	Task("rd.task", func(ctx *Ctx) error { return nil },
		WithRetry(3),
		WithRetryDelay(500*time.Millisecond),
	)
	td := defaultRegistry.lookupTask("rd.task")
	if td == nil {
		t.Fatal("task not registered")
	}
	if td.retryStrategy == nil {
		t.Fatal("expected WithRetryDelay to create a FixedDelay strategy")
	}
	d := td.retryStrategy.Delay(0)
	if d != 500*time.Millisecond {
		t.Fatalf("expected 500ms, got %s", d)
	}
}

func TestEngine_ExponentialBackoff_Integration(t *testing.T) {
	ResetTaskRegistry()

	var attempts int
	Task("eb.task", func(ctx *Ctx) error {
		attempts++
		if attempts < 3 {
			return fmt.Errorf("attempt %d failed", attempts)
		}
		Set(ctx, "attempts", attempts)
		return nil
	}, WithRetry(5), WithRetryStrategy(ExponentialBackoff(10*time.Millisecond, 1*time.Second)))

	wf, err := NewWorkflow("exp-backoff").Step("eb.task").Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	start := time.Now()
	result, err := engine.RunSync(context.Background(), wf, nil)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}

	v, _ := ResultGet[int](result, "attempts")
	if v != 3 {
		t.Fatalf("expected 3 attempts, got %d", v)
	}

	if elapsed > 2*time.Second {
		t.Fatalf("took too long: %s (backoff should be fast for small base)", elapsed)
	}
}

func TestWorkflow_DefaultRetryStrategy(t *testing.T) {
	ResetTaskRegistry()

	var attempts int
	Task("wds.task", func(ctx *Ctx) error {
		attempts++
		if attempts < 3 {
			return fmt.Errorf("attempt %d failed", attempts)
		}
		return nil
	}, WithRetry(5))

	wf, err := NewWorkflow("wf-strategy",
		WithDefaultRetryStrategy(ExponentialBackoff(10*time.Millisecond, 1*time.Second)),
	).Step("wds.task").Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
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
}
