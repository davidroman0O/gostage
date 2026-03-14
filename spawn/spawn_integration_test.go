package spawn_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gs "github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/spawn"
)

// TestMain handles dual-mode execution: normal test runner OR child process.
// When the test binary is spawned by gostage as a child, it enters HandleChild.
// A task middleware is always registered so TestHandleChild_WithTaskMiddleware
// can verify that HandleChild(opts...) correctly applies engine options without
// needing a side-channel environment variable.
func TestMain(m *testing.M) {
	if spawn.IsChild() {
		gs.ResetTaskRegistry()
		registerSpawnTestTasks()
		spawn.HandleChild(
			gs.WithTaskMiddleware(func(tctx *gs.Ctx, taskName string, next func() error) error {
				// Write a marker so TestHandleChild_WithTaskMiddleware can verify
				// that opts passed to HandleChild are applied in the child engine.
				gs.Set(tctx, "child_mw_ran", true)
				return next()
			}),
		)
		// HandleChild calls os.Exit -- unreachable
		return
	}
	os.Exit(m.Run())
}

// registerSpawnTestTasks registers the tasks that child processes need.
func registerSpawnTestTasks() {
	gs.Task("spawn.echo", func(ctx *gs.Ctx) error {
		// Read the ForEach item, write it to a result key
		item := gs.Item[string](ctx)
		idx := gs.ItemIndex(ctx)
		gs.Set(ctx, fmt.Sprintf("result_%d", idx), item)
		gs.Set(ctx, "child_ran", true)
		return nil
	})

	gs.Task("spawn.add", func(ctx *gs.Ctx) error {
		item := gs.Item[float64](ctx)
		idx := gs.ItemIndex(ctx)
		gs.Set(ctx, fmt.Sprintf("sum_%d", idx), item*2)
		return nil
	})

	gs.Task("spawn.crash", func(ctx *gs.Ctx) error {
		return fmt.Errorf("intentional child crash")
	})

	gs.Task("spawn.send", func(ctx *gs.Ctx) error {
		item := gs.Item[string](ctx)
		gs.Send(ctx, "progress", gs.Params{"track": item, "pct": 100})
		gs.Set(ctx, "sent", true)
		return nil
	})

	gs.Task("spawn.slow", func(ctx *gs.Ctx) error {
		time.Sleep(100 * time.Millisecond)
		gs.Set(ctx, "slow_done", true)
		return nil
	})

	// spawn.retry_flaky: fails on first call, succeeds on retry.
	gs.Task("spawn.retry_flaky", func(ctx *gs.Ctx) error {
		markerPath := gs.GetOr[string](ctx, "marker_path", "")
		if markerPath == "" {
			return fmt.Errorf("marker_path not set")
		}
		if _, err := os.Stat(markerPath); os.IsNotExist(err) {
			os.WriteFile(markerPath, []byte("attempted"), 0644)
			return fmt.Errorf("transient failure")
		}
		gs.Set(ctx, "retried", true)
		return nil
	}, gs.WithRetry(1))

	// spawn.mw_echo: simple task for middleware testing
	gs.Task("spawn.mw_echo", func(ctx *gs.Ctx) error {
		gs.Set(ctx, "mw_child_ran", true)
		return nil
	})

	// spawn.bail: calls Bail() to test bail propagation from spawned child
	gs.Task("spawn.bail", func(ctx *gs.Ctx) error {
		return gs.Bail(ctx, "child bail reason")
	})

	// spawn.grandchild.work: leaf task for multi-level spawn tests
	gs.Task("spawn.grandchild.work", func(ctx *gs.Ctx) error {
		item := gs.Item[string](ctx)
		idx := gs.ItemIndex(ctx)
		gs.Set(ctx, fmt.Sprintf("gc_result_%d", idx), item)
		gs.Send(ctx, "grandchild.done", gs.Params{"item": item, "index": idx})
		return nil
	})
}

func TestIsChild(t *testing.T) {
	if spawn.IsChild() {
		t.Fatal("test process should not be detected as child")
	}
}

func TestSpawn_SingleItem(t *testing.T) {
	gs.ResetTaskRegistry()
	gs.Task("spawn.echo", func(ctx *gs.Ctx) error {
		item := gs.Item[string](ctx)
		idx := gs.ItemIndex(ctx)
		gs.Set(ctx, fmt.Sprintf("result_%d", idx), item)
		gs.Set(ctx, "child_ran", true)
		return nil
	})

	wf, err := gs.NewWorkflow("spawn-single").
		ForEach("items", gs.Step("spawn.echo"), gs.WithSpawn()).
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := gs.New(spawn.WithSpawn())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := engine.RunSync(ctx, wf, gs.Params{"items": []string{"hello"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != gs.Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}
}

func TestSpawn_ConcurrentItems(t *testing.T) {
	gs.ResetTaskRegistry()
	gs.Task("spawn.add", func(ctx *gs.Ctx) error {
		item := gs.Item[float64](ctx)
		idx := gs.ItemIndex(ctx)
		gs.Set(ctx, fmt.Sprintf("sum_%d", idx), item*2)
		return nil
	})

	wf, err := gs.NewWorkflow("spawn-concurrent").
		ForEach("numbers", gs.Step("spawn.add"), gs.WithConcurrency(3), gs.WithSpawn()).
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := gs.New(spawn.WithSpawn())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := engine.RunSync(ctx, wf, gs.Params{"numbers": []float64{1, 2, 3, 4}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != gs.Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}
}

func TestSpawn_ChildError(t *testing.T) {
	gs.ResetTaskRegistry()
	gs.Task("spawn.crash", func(ctx *gs.Ctx) error {
		return fmt.Errorf("intentional child crash")
	})

	wf, err := gs.NewWorkflow("spawn-crash").
		ForEach("items", gs.Step("spawn.crash"), gs.WithSpawn()).
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := gs.New(spawn.WithSpawn())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := engine.RunSync(ctx, wf, gs.Params{"items": []string{"fail"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != gs.Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
}

func TestSpawn_ContextCancellation(t *testing.T) {
	gs.ResetTaskRegistry()
	gs.Task("spawn.slow", func(ctx *gs.Ctx) error {
		time.Sleep(100 * time.Millisecond)
		gs.Set(ctx, "slow_done", true)
		return nil
	})

	wf, err := gs.NewWorkflow("spawn-cancel").
		ForEach("items", gs.Step("spawn.slow"), gs.WithSpawn()).
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := gs.New(spawn.WithSpawn())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	result, err := engine.RunSync(ctx, wf, gs.Params{"items": []string{"a", "b", "c", "d", "e", "f", "g", "h"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != gs.Cancelled && result.Status != gs.Failed {
		t.Fatalf("expected Cancelled or Failed, got %s", result.Status)
	}
}

func TestSpawn_SendIPC(t *testing.T) {
	gs.ResetTaskRegistry()
	gs.Task("spawn.send", func(ctx *gs.Ctx) error {
		item := gs.Item[string](ctx)
		gs.Send(ctx, "progress", gs.Params{"track": item, "pct": 100})
		gs.Set(ctx, "sent", true)
		return nil
	})

	wf, err := gs.NewWorkflow("spawn-ipc").
		ForEach("items", gs.Step("spawn.send"), gs.WithSpawn()).
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := gs.New(spawn.WithSpawn())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := engine.RunSync(ctx, wf, gs.Params{"items": []string{"track1"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != gs.Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}
}

func TestSpawn_ForEachItemAccess(t *testing.T) {
	gs.ResetTaskRegistry()

	gs.Task("spawn.echo", func(ctx *gs.Ctx) error {
		item := gs.Item[string](ctx)
		idx := gs.ItemIndex(ctx)
		gs.Set(ctx, fmt.Sprintf("result_%d", idx), item)
		gs.Set(ctx, "child_ran", true)
		return nil
	})

	wf, err := gs.NewWorkflow("spawn-foreach-item").
		ForEach("items", gs.Step("spawn.echo"), gs.WithConcurrency(2), gs.WithSpawn()).
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := gs.New(spawn.WithSpawn())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := engine.RunSync(ctx, wf, gs.Params{"items": []string{"alpha", "beta", "gamma"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != gs.Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}
}

func TestSpawn_EndToEnd(t *testing.T) {
	gs.ResetTaskRegistry()

	gs.Task("spawn.prepare", func(ctx *gs.Ctx) error {
		gs.Set(ctx, "items", []string{"one", "two", "three"})
		gs.Set(ctx, "prepared", true)
		return nil
	})

	gs.Task("spawn.echo", func(ctx *gs.Ctx) error {
		item := gs.Item[string](ctx)
		idx := gs.ItemIndex(ctx)
		gs.Set(ctx, fmt.Sprintf("result_%d", idx), item)
		gs.Set(ctx, "child_ran", true)
		return nil
	})

	gs.Task("spawn.finalize", func(ctx *gs.Ctx) error {
		gs.Set(ctx, "finalized", true)
		return nil
	})

	wf, err := gs.NewWorkflow("spawn-e2e").
		Step("spawn.prepare").
		ForEach("items", gs.Step("spawn.echo"), gs.WithConcurrency(2), gs.WithSpawn()).
		Step("spawn.finalize").
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := gs.New(spawn.WithSpawn())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := engine.RunSync(ctx, wf, gs.Params{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != gs.Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}

	if result.Store["prepared"] != true {
		t.Fatal("expected prepared to be true")
	}
	if result.Store["finalized"] != true {
		t.Fatal("expected finalized to be true")
	}
}

func TestSpawn_EmptyCollection(t *testing.T) {
	gs.ResetTaskRegistry()
	gs.Task("spawn.echo", func(ctx *gs.Ctx) error {
		gs.Set(ctx, "child_ran", true)
		return nil
	})

	wf, err := gs.NewWorkflow("spawn-empty").
		ForEach("items", gs.Step("spawn.echo"), gs.WithSpawn()).
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := gs.New(spawn.WithSpawn())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()
	result, err := engine.RunSync(ctx, wf, gs.Params{"items": []string{}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != gs.Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
}

func TestSpawn_AllFourMiddlewareLevels(t *testing.T) {
	gs.ResetTaskRegistry()

	gs.Task("spawn.mw_setup", func(ctx *gs.Ctx) error {
		gs.Set(ctx, "setup_ran", true)
		return nil
	})
	gs.Task("spawn.mw_echo", func(ctx *gs.Ctx) error {
		gs.Set(ctx, "mw_child_ran", true)
		return nil
	})

	var engineMW, stepMW, taskMW, childMW int32

	engine, err := gs.New(
		spawn.WithSpawn(),
		gs.WithEngineMiddleware(func(ctx context.Context, wf *gs.Workflow, runID gs.RunID, next func() error) error {
			atomic.AddInt32(&engineMW, 1)
			return next()
		}),
		gs.WithStepMiddleware(func(ctx context.Context, info gs.StepInfo, runID gs.RunID, next func() error) error {
			atomic.AddInt32(&stepMW, 1)
			return next()
		}),
		gs.WithTaskMiddleware(func(tctx *gs.Ctx, taskName string, next func() error) error {
			atomic.AddInt32(&taskMW, 1)
			return next()
		}),
		gs.WithChildMiddleware(func(ctx context.Context, job *gs.SpawnJob, next func() error) error {
			atomic.AddInt32(&childMW, 1)
			return next()
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	wf, err := gs.NewWorkflow("mw-all-four").
		Step("spawn.mw_setup").
		ForEach("items", gs.Step("spawn.mw_echo"), gs.WithSpawn()).
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := engine.RunSync(ctx, wf, gs.Params{"items": []string{"a", "b"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != gs.Completed {
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
	gs.ResetTaskRegistry()

	dir := t.TempDir()
	markerPath := filepath.Join(dir, "retry_marker")

	gs.Task("spawn.retry_flaky", func(ctx *gs.Ctx) error {
		mp := gs.GetOr[string](ctx, "marker_path", "")
		if mp == "" {
			return fmt.Errorf("marker_path not set")
		}
		if _, err := os.Stat(mp); os.IsNotExist(err) {
			os.WriteFile(mp, []byte("attempted"), 0644)
			return fmt.Errorf("transient failure")
		}
		gs.Set(ctx, "retried", true)
		return nil
	}, gs.WithRetry(1))

	wf, err := gs.NewWorkflow("spawn-retry").
		ForEach("items", gs.Step("spawn.retry_flaky"), gs.WithSpawn()).
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := gs.New(spawn.WithSpawn())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := engine.RunSync(ctx, wf, gs.Params{
		"items":       []string{"x"},
		"marker_path": markerPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != gs.Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}

	if result.Store["retried"] != true {
		t.Fatal("expected 'retried' to be true -- child retry was not respected")
	}
}

func TestSpawn_MultiLevel_StoreRoundTrip(t *testing.T) {
	gs.ResetTaskRegistry()

	gs.Task("spawn.grandchild.work", func(ctx *gs.Ctx) error {
		item := gs.Item[string](ctx)
		idx := gs.ItemIndex(ctx)
		gs.Set(ctx, fmt.Sprintf("gc_result_%d", idx), item)
		gs.Send(ctx, "grandchild.done", gs.Params{"item": item, "index": idx})
		return nil
	})

	inner, err := gs.NewWorkflow("ml-inner").
		ForEach("subitems", gs.Step("spawn.grandchild.work"), gs.WithConcurrency(1), gs.WithSpawn()).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	outer, err := gs.NewWorkflow("ml-outer").
		ForEach("items", gs.Sub(inner), gs.WithConcurrency(1), gs.WithSpawn()).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := gs.New(spawn.WithSpawn())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	var gcMessages []string
	var gcMu sync.Mutex
	engine.OnMessage("grandchild.done", func(msgType string, payload map[string]any) {
		gcMu.Lock()
		gcMessages = append(gcMessages, fmt.Sprintf("%v", payload["item"]))
		gcMu.Unlock()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := engine.RunSync(ctx, outer, gs.Params{
		"items":    []string{"parent1"},
		"subitems": []string{"gc1", "gc2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != gs.Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}

	if result.Store["gc_result_0"] == nil {
		t.Fatal("expected gc_result_0 in parent store (grandchild store merge)")
	}

	gcMu.Lock()
	msgCount := len(gcMessages)
	gcMu.Unlock()
	if msgCount == 0 {
		t.Fatal("expected IPC messages forwarded from grandchild to parent via two gRPC hops")
	}
}

func TestSpawn_BailFromChild(t *testing.T) {
	gs.ResetTaskRegistry()

	gs.Task("spawn.bail", func(ctx *gs.Ctx) error {
		return gs.Bail(ctx, "child bail reason")
	})

	wf, err := gs.NewWorkflow("spawn-bail").
		ForEach("items", gs.Step("spawn.bail"), gs.WithSpawn()).
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := gs.New(spawn.WithSpawn())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := engine.RunSync(ctx, wf, gs.Params{"items": []string{"x"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != gs.Bailed {
		t.Fatalf("expected Bailed, got %s (error: %v)", result.Status, result.Error)
	}
	if result.BailReason != "child bail reason" {
		t.Fatalf("expected bail reason 'child bail reason', got %q", result.BailReason)
	}
}

func TestSpawn_PanicInSpawnGoroutine(t *testing.T) {
	gs.ResetTaskRegistry()

	gs.Task("spawn.echo", func(ctx *gs.Ctx) error {
		item := gs.Item[string](ctx)
		idx := gs.ItemIndex(ctx)
		gs.Set(ctx, fmt.Sprintf("result_%d", idx), item)
		gs.Set(ctx, "child_ran", true)
		return nil
	})

	wf, err := gs.NewWorkflow("spawn-panic-mw").
		ForEach("items", gs.Step("spawn.echo"), gs.WithConcurrency(2), gs.WithSpawn()).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := gs.New(
		spawn.WithSpawn(),
		gs.WithChildMiddleware(func(ctx context.Context, job *gs.SpawnJob, next func() error) error {
			panic("intentional child middleware panic")
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := engine.RunSync(ctx, wf, gs.Params{"items": []string{"a", "b"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != gs.Failed {
		t.Fatalf("expected Failed (panic recovered), got %s", result.Status)
	}
	if result.Error == nil {
		t.Fatal("expected error from panic recovery")
	}
}

func TestSpawnDepth_LimitEnforced(t *testing.T) {
	gs.ResetTaskRegistry()

	gs.Task("spawn.echo", func(ctx *gs.Ctx) error {
		item := gs.Item[string](ctx)
		idx := gs.ItemIndex(ctx)
		gs.Set(ctx, fmt.Sprintf("result_%d", idx), item)
		gs.Set(ctx, "child_ran", true)
		return nil
	})

	wf, err := gs.NewWorkflow("spawn-depth-limit").
		ForEach("items", gs.Step("spawn.echo"), gs.WithSpawn()).
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := gs.New(spawn.WithSpawn())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	t.Setenv("GOSTAGE_MAX_SPAWN_DEPTH", "0")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := engine.RunSync(ctx, wf, gs.Params{"items": []string{"x"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != gs.Failed {
		t.Fatalf("expected Failed (depth limit exceeded), got %s", result.Status)
	}
}

func TestHandleChild_WithTaskMiddleware(t *testing.T) {
	gs.ResetTaskRegistry()

	gs.Task("spawn.echo", func(ctx *gs.Ctx) error {
		item := gs.Item[string](ctx)
		idx := gs.ItemIndex(ctx)
		gs.Set(ctx, fmt.Sprintf("result_%d", idx), item)
		gs.Set(ctx, "child_ran", true)
		return nil
	})

	wf, err := gs.NewWorkflow("child-mw-test").
		ForEach("items", gs.Step("spawn.echo"), gs.WithSpawn()).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := gs.New(spawn.WithSpawn())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := engine.RunSync(ctx, wf, gs.Params{"items": []string{"hello"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != gs.Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}
	if result.Store["child_mw_ran"] != true {
		t.Fatal("expected child_mw_ran=true, indicating task middleware fired in child process")
	}
}

func TestStoreSerializationRoundTrip(t *testing.T) {
	// Use exported API for serialization round-trip testing
	s := gs.NewRunStateForChild("test")
	s.SetClean("name", "Alice")
	s.SetClean("age", 30)
	s.SetClean("scores", []int{90, 85, 92})

	// Dirty the entries so SerializeAll includes them
	// (SetClean doesn't mark dirty, but SerializeAll returns all entries)
	data, err := gs.SerializeStateForChild(s.Accessor(), "item-value", 7)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := data["__foreach_item"]; !ok {
		t.Fatal("expected __foreach_item in serialized data")
	}
	if _, ok := data["__foreach_index"]; !ok {
		t.Fatal("expected __foreach_index in serialized data")
	}

	vals, err := gs.DeserializeStoreData(data)
	if err != nil {
		t.Fatal(err)
	}

	if vals["name"] != "Alice" {
		t.Fatalf("expected Alice, got %v", vals["name"])
	}
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
