package gostage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestMain handles dual-mode execution: normal test runner OR child process.
// When the test binary is spawned by gostage as a child, it enters HandleChild.
func TestMain(m *testing.M) {
	if IsChild() {
		ResetTaskRegistry()
		registerSpawnTestTasks()
		HandleChild()
		// HandleChild calls os.Exit — unreachable
		return
	}
	os.Exit(m.Run())
}

// registerSpawnTestTasks registers the tasks that child processes need.
func registerSpawnTestTasks() {
	Task("spawn.echo", func(ctx *Ctx) error {
		// Read the ForEach item, write it to a result key
		item := Item[string](ctx)
		idx := ItemIndex(ctx)
		Set(ctx, fmt.Sprintf("result_%d", idx), item)
		Set(ctx, "child_ran", true)
		return nil
	})

	Task("spawn.add", func(ctx *Ctx) error {
		item := Item[float64](ctx)
		idx := ItemIndex(ctx)
		Set(ctx, fmt.Sprintf("sum_%d", idx), item*2)
		return nil
	})

	Task("spawn.crash", func(ctx *Ctx) error {
		return fmt.Errorf("intentional child crash")
	})

	Task("spawn.send", func(ctx *Ctx) error {
		item := Item[string](ctx)
		Send(ctx, "progress", P{"track": item, "pct": 100})
		Set(ctx, "sent", true)
		return nil
	})

	Task("spawn.slow", func(ctx *Ctx) error {
		time.Sleep(100 * time.Millisecond)
		Set(ctx, "slow_done", true)
		return nil
	})

	// spawn.retry_flaky: fails on first call, succeeds on retry.
	// Uses a temp file marker to track attempts within the child process.
	Task("spawn.retry_flaky", func(ctx *Ctx) error {
		markerPath := GetOr[string](ctx, "marker_path", "")
		if markerPath == "" {
			return fmt.Errorf("marker_path not set")
		}
		if _, err := os.Stat(markerPath); os.IsNotExist(err) {
			// First attempt: create marker, fail
			os.WriteFile(markerPath, []byte("attempted"), 0644)
			return fmt.Errorf("transient failure")
		}
		// Retry attempt: marker exists, succeed
		Set(ctx, "retried", true)
		return nil
	}, WithRetry(1))

	// spawn.mw_echo: simple task for middleware testing
	Task("spawn.mw_echo", func(ctx *Ctx) error {
		Set(ctx, "mw_child_ran", true)
		return nil
	})
}

func TestIsChild(t *testing.T) {
	// Current process should NOT be a child (running as test)
	// IsChild checks os.Args for --gostage-child
	if IsChild() {
		t.Fatal("test process should not be detected as child")
	}
}

func TestStoreSerializationRoundTrip(t *testing.T) {
	s := newRunState("test", nil)
	s.Set("name", "Alice")
	s.Set("age", 30)
	s.Set("scores", []int{90, 85, 92})

	// Serialize
	data, err := serializeStateForChild(s, "item-value", 7)
	if err != nil {
		t.Fatal(err)
	}

	// Check item and index are included
	if _, ok := data["__foreach_item"]; !ok {
		t.Fatal("expected __foreach_item in serialized data")
	}
	if _, ok := data["__foreach_index"]; !ok {
		t.Fatal("expected __foreach_index in serialized data")
	}

	// Deserialize
	vals, err := deserializeStoreData(data)
	if err != nil {
		t.Fatal(err)
	}

	if vals["name"] != "Alice" {
		t.Fatalf("expected Alice, got %v", vals["name"])
	}
	// Type-preserved round-trip: int stays int (not float64)
	if vals["age"] != int(30) {
		t.Fatalf("expected int(30), got %T(%v)", vals["age"], vals["age"])
	}
	if vals["__foreach_item"] != "item-value" {
		t.Fatalf("expected item-value, got %v", vals["__foreach_item"])
	}
	if vals["__foreach_index"] != int(7) {
		t.Fatalf("expected int(7), got %T(%v)", vals["__foreach_index"], vals["__foreach_index"])
	}
}

func TestSpawnServer_StartStop(t *testing.T) {
	ss, err := newSpawnServer(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.stop()

	if ss.port == 0 {
		t.Fatal("expected non-zero port")
	}
}

func TestSpawn_SingleItem(t *testing.T) {
	ResetTaskRegistry()
	Task("spawn.echo", func(ctx *Ctx) error {
		item := Item[string](ctx)
		idx := ItemIndex(ctx)
		Set(ctx, fmt.Sprintf("result_%d", idx), item)
		Set(ctx, "child_ran", true)
		return nil
	})

	wf := NewWorkflow("spawn-single").
		ForEach("items", Step("spawn.echo"), WithSpawn()).
		Commit()

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := engine.RunSync(ctx, wf, P{"items": []string{"hello"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}
}

func TestSpawn_ConcurrentItems(t *testing.T) {
	ResetTaskRegistry()
	Task("spawn.add", func(ctx *Ctx) error {
		item := Item[float64](ctx)
		idx := ItemIndex(ctx)
		Set(ctx, fmt.Sprintf("sum_%d", idx), item*2)
		return nil
	})

	wf := NewWorkflow("spawn-concurrent").
		ForEach("numbers", Step("spawn.add"), WithConcurrency(3), WithSpawn()).
		Commit()

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := engine.RunSync(ctx, wf, P{"numbers": []float64{1, 2, 3, 4}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}
}

func TestSpawn_ChildError(t *testing.T) {
	ResetTaskRegistry()
	Task("spawn.crash", func(ctx *Ctx) error {
		return fmt.Errorf("intentional child crash")
	})

	wf := NewWorkflow("spawn-crash").
		ForEach("items", Step("spawn.crash"), WithSpawn()).
		Commit()

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := engine.RunSync(ctx, wf, P{"items": []string{"fail"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
}

func TestSpawn_ContextCancellation(t *testing.T) {
	ResetTaskRegistry()
	Task("spawn.slow", func(ctx *Ctx) error {
		time.Sleep(100 * time.Millisecond)
		Set(ctx, "slow_done", true)
		return nil
	})

	wf := NewWorkflow("spawn-cancel").
		ForEach("items", Step("spawn.slow"), WithSpawn()).
		Commit()

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Cancel almost immediately
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	result, err := engine.RunSync(ctx, wf, P{"items": []string{"a", "b", "c", "d", "e", "f", "g", "h"}})
	if err != nil {
		t.Fatal(err)
	}
	// Should be Cancelled or Failed due to context cancellation
	if result.Status != Cancelled && result.Status != Failed {
		t.Fatalf("expected Cancelled or Failed, got %s", result.Status)
	}
}

func TestSpawn_SendIPC(t *testing.T) {
	ResetTaskRegistry()
	Task("spawn.send", func(ctx *Ctx) error {
		item := Item[string](ctx)
		Send(ctx, "progress", P{"track": item, "pct": 100})
		Set(ctx, "sent", true)
		return nil
	})

	wf := NewWorkflow("spawn-ipc").
		ForEach("items", Step("spawn.send"), WithSpawn()).
		Commit()

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := engine.RunSync(ctx, wf, P{"items": []string{"track1"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}
}

func TestSpawn_ForEachItemAccess(t *testing.T) {
	ResetTaskRegistry()

	Task("spawn.echo", func(ctx *Ctx) error {
		item := Item[string](ctx)
		idx := ItemIndex(ctx)
		Set(ctx, fmt.Sprintf("result_%d", idx), item)
		Set(ctx, "child_ran", true)
		return nil
	})

	wf := NewWorkflow("spawn-foreach-item").
		ForEach("items", Step("spawn.echo"), WithConcurrency(2), WithSpawn()).
		Commit()

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := engine.RunSync(ctx, wf, P{"items": []string{"alpha", "beta", "gamma"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}
}

func TestSpawn_EndToEnd(t *testing.T) {
	ResetTaskRegistry()

	Task("spawn.prepare", func(ctx *Ctx) error {
		Set(ctx, "items", []string{"one", "two", "three"})
		Set(ctx, "prepared", true)
		return nil
	})

	Task("spawn.echo", func(ctx *Ctx) error {
		item := Item[string](ctx)
		idx := ItemIndex(ctx)
		Set(ctx, fmt.Sprintf("result_%d", idx), item)
		Set(ctx, "child_ran", true)
		return nil
	})

	Task("spawn.finalize", func(ctx *Ctx) error {
		Set(ctx, "finalized", true)
		return nil
	})

	wf := NewWorkflow("spawn-e2e").
		Step("spawn.prepare").
		ForEach("items", Step("spawn.echo"), WithConcurrency(2), WithSpawn()).
		Step("spawn.finalize").
		Commit()

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := engine.RunSync(ctx, wf, P{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}

	// Verify the prepare and finalize steps ran
	if result.Store["prepared"] != true {
		t.Fatal("expected prepared to be true")
	}
	if result.Store["finalized"] != true {
		t.Fatal("expected finalized to be true")
	}
}

func TestSpawn_EmptyCollection(t *testing.T) {
	ResetTaskRegistry()
	Task("spawn.echo", func(ctx *Ctx) error {
		Set(ctx, "child_ran", true)
		return nil
	})

	wf := NewWorkflow("spawn-empty").
		ForEach("items", Step("spawn.echo"), WithSpawn()).
		Commit()

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()
	result, err := engine.RunSync(ctx, wf, P{"items": []string{}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
}

func TestSpawn_AllFourMiddlewareLevels(t *testing.T) {
	ResetTaskRegistry()

	// Register tasks: one normal step (triggers task middleware) + one spawn step (triggers child middleware)
	Task("spawn.mw_setup", func(ctx *Ctx) error {
		Set(ctx, "setup_ran", true)
		return nil
	})
	Task("spawn.mw_echo", func(ctx *Ctx) error {
		Set(ctx, "mw_child_ran", true)
		return nil
	})

	var engineMW, stepMW, taskMW, childMW int32

	engine, err := New(
		WithEngineMiddleware(func(ctx context.Context, wf *Workflow, runID RunID, next func() error) error {
			atomic.AddInt32(&engineMW, 1)
			return next()
		}),
		WithStepMiddleware(func(ctx context.Context, s *step, runID RunID, next func() error) error {
			atomic.AddInt32(&stepMW, 1)
			return next()
		}),
		WithTaskMiddleware(func(tctx *Ctx, taskName string, next func() error) error {
			atomic.AddInt32(&taskMW, 1)
			return next()
		}),
		WithChildMiddleware(func(ctx context.Context, job *SpawnJob, next func() error) error {
			atomic.AddInt32(&childMW, 1)
			return next()
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// Workflow: normal task step + ForEach with spawn
	wf := NewWorkflow("mw-all-four").
		Step("spawn.mw_setup").
		ForEach("items", Step("spawn.mw_echo"), WithSpawn()).
		Commit()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := engine.RunSync(ctx, wf, P{"items": []string{"a", "b"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}

	if atomic.LoadInt32(&engineMW) < 1 {
		t.Fatal("engine middleware did not fire")
	}
	if atomic.LoadInt32(&stepMW) < 1 {
		t.Fatal("step middleware did not fire")
	}
	if atomic.LoadInt32(&taskMW) < 1 {
		t.Fatal("task middleware did not fire")
	}
	if atomic.LoadInt32(&childMW) < 2 {
		t.Fatalf("expected child middleware to fire >= 2 times (once per spawn item), got %d", atomic.LoadInt32(&childMW))
	}
}

func TestSpawn_ChildRetryRespected(t *testing.T) {
	ResetTaskRegistry()

	// Create a temp file path for the marker
	dir := t.TempDir()
	markerPath := filepath.Join(dir, "retry_marker")

	// Register task with retry — fails first call, succeeds on retry
	Task("spawn.retry_flaky", func(ctx *Ctx) error {
		mp := GetOr[string](ctx, "marker_path", "")
		if mp == "" {
			return fmt.Errorf("marker_path not set")
		}
		if _, err := os.Stat(mp); os.IsNotExist(err) {
			os.WriteFile(mp, []byte("attempted"), 0644)
			return fmt.Errorf("transient failure")
		}
		Set(ctx, "retried", true)
		return nil
	}, WithRetry(1))

	wf := NewWorkflow("spawn-retry").
		ForEach("items", Step("spawn.retry_flaky"), WithSpawn()).
		Commit()

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := engine.RunSync(ctx, wf, P{
		"items":       []string{"x"},
		"marker_path": markerPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}

	// The child should have retried and set "retried" = true
	if result.Store["retried"] != true {
		t.Fatal("expected 'retried' to be true — child retry was not respected")
	}
}
