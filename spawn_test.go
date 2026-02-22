package gostage

import (
	"context"
	"fmt"
	"os"
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
	// JSON round-trip: int becomes float64
	if vals["age"] != float64(30) {
		t.Fatalf("expected 30, got %v", vals["age"])
	}
	if vals["__foreach_item"] != "item-value" {
		t.Fatalf("expected item-value, got %v", vals["__foreach_item"])
	}
	if vals["__foreach_index"] != float64(7) {
		t.Fatalf("expected 7, got %v", vals["__foreach_index"])
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
