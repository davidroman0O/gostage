package gostage

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// === Worker Pool ===

func TestWorkerPool(t *testing.T) {
	p := newWorkerPool(4)
	defer p.Shutdown(0)

	if p.Size() != 4 {
		t.Fatalf("expected pool size 4, got %d", p.Size())
	}

	var count int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		ok := p.Submit(func() {
			defer wg.Done()
			atomic.AddInt32(&count, 1)
			time.Sleep(10 * time.Millisecond)
		})
		if !ok {
			t.Fatal("submit should succeed")
		}
	}
	wg.Wait()

	if atomic.LoadInt32(&count) != 20 {
		t.Fatalf("expected 20 completions, got %d", count)
	}
}

func TestWorkerPoolShutdown(t *testing.T) {
	p := newWorkerPool(2)
	p.Shutdown(0)

	// Submit after shutdown should return false
	ok := p.Submit(func() {})
	if ok {
		t.Fatal("submit after shutdown should return false")
	}
}

func TestPoolBoundsWorkflows(t *testing.T) {
	ResetTaskRegistry()

	var maxConcurrent int32
	var current int32

	Task("pool.slow", func(ctx *Ctx) error {
		c := atomic.AddInt32(&current, 1)
		for {
			old := atomic.LoadInt32(&maxConcurrent)
			if c <= old || atomic.CompareAndSwapInt32(&maxConcurrent, old, c) {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
		atomic.AddInt32(&current, -1)
		return nil
	})

	// Pool of 2 workers — only 2 workflows can execute at once
	engine, err := New(WithWorkerPoolSize(2))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	wf, err := NewWorkflow("pool-bounded").Step("pool.slow").Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Start 6 workflows async
	var ids []RunID
	for i := 0; i < 6; i++ {
		id, err := engine.Run(context.Background(), wf, nil)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}

	// Wait for all to complete
	for _, id := range ids {
		result, err := engine.Wait(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if result.Status != Completed {
			t.Fatalf("expected completed, got %s: %v", result.Status, result.Error)
		}
	}

	// Peak concurrency should be ≤ pool size (2)
	peak := atomic.LoadInt32(&maxConcurrent)
	if peak > 2 {
		t.Fatalf("expected max concurrent workflows ≤ 2, got %d", peak)
	}
	if peak < 1 {
		t.Fatal("expected at least 1 concurrent execution")
	}
}
