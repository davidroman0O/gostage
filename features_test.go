package gostage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/davidroman0O/gostage/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// === Orphan Detection (unit tests) ===

func TestOrphanWatcherPID(t *testing.T) {
	// This tests that the PID watcher goroutine starts and doesn't panic
	ctx, cancel := context.WithCancel(context.Background())
	stop := startOrphanWatcher(ctx, cancel, nil)
	defer stop()

	// Should not have cancelled yet (parent is still alive)
	select {
	case <-ctx.Done():
		t.Fatal("context should not be cancelled while parent is alive")
	case <-time.After(100 * time.Millisecond):
		// Good - parent is still alive
	}

	cancel() // clean up
}

// === Spawn Type Preservation ===

func TestSpawnTypePreservation(t *testing.T) {
	// Verify that the serialize->deserialize round-trip preserves type info
	s := newRunState("test-spawn-types", nil)
	s.Set("count", 42)
	s.Set("name", "hello")
	s.Set("ratio", 3.14)
	s.Set("flag", true)
	s.Set("big", int64(9999999999))

	// Simulate parent->child serialization (with a ForEach item)
	data, err := serializeStateForChild(s, "item-val", 5)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	// Simulate child->parent deserialization
	result, err := deserializeStoreData(data)
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}

	// int must survive round-trip (not become float64)
	if v, ok := result["count"].(int); !ok || v != 42 {
		t.Fatalf("count: expected int(42), got %T(%v)", result["count"], result["count"])
	}

	// string must survive
	if v, ok := result["name"].(string); !ok || v != "hello" {
		t.Fatalf("name: expected string(hello), got %T(%v)", result["name"], result["name"])
	}

	// float64 must survive
	if v, ok := result["ratio"].(float64); !ok || v != 3.14 {
		t.Fatalf("ratio: expected float64(3.14), got %T(%v)", result["ratio"], result["ratio"])
	}

	// bool must survive
	if v, ok := result["flag"].(bool); !ok || v != true {
		t.Fatalf("flag: expected bool(true), got %T(%v)", result["flag"], result["flag"])
	}

	// int64 must survive
	if v, ok := result["big"].(int64); !ok || v != 9999999999 {
		t.Fatalf("big: expected int64(9999999999), got %T(%v)", result["big"], result["big"])
	}

	// ForEach item must survive
	if v, ok := result["__foreach_item"].(string); !ok || v != "item-val" {
		t.Fatalf("__foreach_item: expected string(item-val), got %T(%v)", result["__foreach_item"], result["__foreach_item"])
	}

	// ForEach index must be int (not float64)
	if v, ok := result["__foreach_index"].(int); !ok || v != 5 {
		t.Fatalf("__foreach_index: expected int(5), got %T(%v)", result["__foreach_index"], result["__foreach_index"])
	}
}

func TestSpawnDirtyTypePreservation(t *testing.T) {
	// Verify that SerializeDirty->deserialize round-trip preserves types
	s := newRunState("test-dirty", nil)
	s.SetClean("existing", "parent-data")   // not dirty
	s.Set("child_wrote", 99)                 // dirty
	s.Set("child_flag", true)                // dirty

	data, err := s.SerializeDirty()
	if err != nil {
		t.Fatalf("SerializeDirty: %v", err)
	}

	// Should only contain dirty entries
	if _, has := data["existing"]; has {
		t.Fatal("expected 'existing' to be excluded (not dirty)")
	}

	result, err := deserializeStoreData(data)
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}

	if v, ok := result["child_wrote"].(int); !ok || v != 99 {
		t.Fatalf("child_wrote: expected int(99), got %T(%v)", result["child_wrote"], result["child_wrote"])
	}
	if v, ok := result["child_flag"].(bool); !ok || v != true {
		t.Fatalf("child_flag: expected bool(true), got %T(%v)", result["child_flag"], result["child_flag"])
	}
}

// === Decision 003 Tests ===

// TestMiddlewareOrdering verifies the last-registered middleware executes first
// (wraps outermost), matching the INTENT specification.
func TestMiddlewareOrdering(t *testing.T) {
	ResetTaskRegistry()

	Task("mw.order.task", func(ctx *Ctx) error {
		Set(ctx, "done", true)
		return nil
	})

	var order []string
	var mu sync.Mutex

	engine, err := New(
		WithEngineMiddleware(func(ctx context.Context, wf *Workflow, runID RunID, next func() error) error {
			mu.Lock()
			order = append(order, "engine_first_enter")
			mu.Unlock()
			err := next()
			mu.Lock()
			order = append(order, "engine_first_exit")
			mu.Unlock()
			return err
		}),
		WithEngineMiddleware(func(ctx context.Context, wf *Workflow, runID RunID, next func() error) error {
			mu.Lock()
			order = append(order, "engine_last_enter")
			mu.Unlock()
			err := next()
			mu.Lock()
			order = append(order, "engine_last_exit")
			mu.Unlock()
			return err
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	wf, err := NewWorkflow("mw-order").Step("mw.order.task").Commit()
	if err != nil {
		t.Fatal(err)
	}
	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}

	// Last-registered should be outermost: enters first, exits last
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 4 {
		t.Fatalf("expected 4 middleware events, got %d: %v", len(order), order)
	}
	if order[0] != "engine_last_enter" {
		t.Fatalf("expected last-registered MW to enter first, got %v", order)
	}
	if order[3] != "engine_last_exit" {
		t.Fatalf("expected last-registered MW to exit last, got %v", order)
	}
}

// TestEngineDoubleClose verifies calling Close() twice does not panic.
func TestEngineDoubleClose(t *testing.T) {
	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}

	// First close should succeed
	if err := engine.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	// Second close should be a no-op, not panic
	if err := engine.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// === Task 11: Concurrent Mutations from Parallel Steps ===

func TestConcurrentMutationsFromParallel(t *testing.T) {
	ResetTaskRegistry()

	var mu sync.Mutex
	var order []string

	Task("cmp.par_a", func(ctx *Ctx) error {
		mu.Lock()
		order = append(order, "par_a")
		mu.Unlock()
		InsertAfter(ctx, "cmp.dynamic_a")
		return nil
	})
	Task("cmp.par_b", func(ctx *Ctx) error {
		mu.Lock()
		order = append(order, "par_b")
		mu.Unlock()
		InsertAfter(ctx, "cmp.dynamic_b")
		return nil
	})
	Task("cmp.dynamic_a", func(ctx *Ctx) error {
		mu.Lock()
		order = append(order, "dynamic_a")
		mu.Unlock()
		return nil
	})
	Task("cmp.dynamic_b", func(ctx *Ctx) error {
		mu.Lock()
		order = append(order, "dynamic_b")
		mu.Unlock()
		return nil
	})
	Task("cmp.post", func(ctx *Ctx) error {
		mu.Lock()
		order = append(order, "post")
		mu.Unlock()
		Set(ctx, "post_ran", true)
		return nil
	})

	wf, err := NewWorkflow("conc-mutations").
		Parallel(Step("cmp.par_a"), Step("cmp.par_b")).
		Step("cmp.post").
		Commit()
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
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}

	// Both parallel tasks should have run
	mu.Lock()
	hasParA, hasParB, hasDynA, hasDynB, hasPost := false, false, false, false, false
	for _, s := range order {
		switch s {
		case "par_a":
			hasParA = true
		case "par_b":
			hasParB = true
		case "dynamic_a":
			hasDynA = true
		case "dynamic_b":
			hasDynB = true
		case "post":
			hasPost = true
		}
	}
	mu.Unlock()

	if !hasParA || !hasParB {
		t.Fatalf("expected both parallel tasks to run, got order: %v", order)
	}
	if !hasDynA || !hasDynB {
		t.Fatalf("expected both dynamic mutations to execute, got order: %v", order)
	}
	if !hasPost {
		t.Fatalf("expected post step to run, got order: %v", order)
	}
}

// === Task 12: Cancel During Retry Delay ===

func TestRetryCancellation(t *testing.T) {
	ResetTaskRegistry()

	var attempts int32

	Task("rc.always_fail", func(ctx *Ctx) error {
		atomic.AddInt32(&attempts, 1)
		return fmt.Errorf("always fails")
	}, WithRetry(5), WithRetryDelay(500*time.Millisecond))

	wf, err := NewWorkflow("retry-cancel").Step("rc.always_fail").Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	runID, err := engine.Run(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Let first attempt fail and enter retry delay
	time.Sleep(150 * time.Millisecond)

	// Cancel during retry delay
	if err := engine.Cancel(context.Background(), runID); err != nil {
		t.Fatal(err)
	}

	result, err := engine.Wait(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}

	if result.Status != Cancelled && result.Status != Failed {
		t.Fatalf("expected Cancelled or Failed, got %s", result.Status)
	}

	// Should not have exhausted all retries
	finalAttempts := atomic.LoadInt32(&attempts)
	if finalAttempts >= 6 { // 1 initial + 5 retries = 6 max
		t.Fatalf("expected fewer than 6 attempts (cancelled during retry), got %d", finalAttempts)
	}
}

// === Task 13: Panic Recovery for User Functions ===

func TestMapPanicRecovery(t *testing.T) {
	ResetTaskRegistry()

	wf, err := NewWorkflow("map-panic").
		Map(func(ctx *Ctx) error {
			panic("map boom")
		}).
		Commit()
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
	if result.Status != Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
	if result.Error == nil {
		t.Fatal("expected error to be set")
	}
}

func TestMapErrorPropagation(t *testing.T) {
	ResetTaskRegistry()

	wf, err := NewWorkflow("map-error").
		Map(func(ctx *Ctx) error {
			return fmt.Errorf("transform failed: bad data")
		}).
		Commit()
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
	if result.Status != Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
	if result.Error == nil {
		t.Fatal("expected error to be set")
	}
	if !strings.Contains(result.Error.Error(), "transform failed") {
		t.Fatalf("expected 'transform failed' in error, got: %v", result.Error)
	}
}

func TestBranchConditionPanicRecovery(t *testing.T) {
	ResetTaskRegistry()

	Task("bcp.task", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("branch-cond-panic").
		Branch(
			When(func(ctx *Ctx) bool { panic("cond boom") }).Step("bcp.task"),
		).
		Commit()
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
	if result.Status != Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
	if result.Error == nil {
		t.Fatal("expected error to be set")
	}
}

func TestLoopConditionPanicRecovery(t *testing.T) {
	ResetTaskRegistry()

	Task("lcp.task", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("loop-cond-panic").
		DoUntil(Step("lcp.task"), func(ctx *Ctx) bool { panic("loop boom") }).
		Commit()
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
	if result.Status != Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
	if result.Error == nil {
		t.Fatal("expected error to be set")
	}
}

// === Decision 006: Bug Fixes and Test Gaps ===

// TestRunAndResumeAfterClose verifies that Run and Resume reject work after engine.Close().
func TestRunAndResumeAfterClose(t *testing.T) {
	ResetTaskRegistry()

	Task("rac.task", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("run-after-close").
		Step("rac.task").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	// Run after Close should return error
	_, runErr := engine.Run(context.Background(), wf, nil)
	if runErr == nil {
		t.Fatal("expected Run to fail after Close")
	}
	if !errors.Is(runErr, ErrEngineClosed) {
		t.Fatalf("expected ErrEngineClosed, got: %v", runErr)
	}

	// Resume after Close should return error
	_, resumeErr := engine.Resume(context.Background(), wf, "fake-run-id", nil)
	if resumeErr == nil {
		t.Fatal("expected Resume to fail after Close")
	}
	if !errors.Is(resumeErr, ErrEngineClosed) {
		t.Fatalf("expected ErrEngineClosed, got: %v", resumeErr)
	}
}

// TestTaskPanicWithoutMiddleware verifies panic recovery in the no-middleware task invocation path.
func TestTaskPanicWithoutMiddleware(t *testing.T) {
	ResetTaskRegistry()

	Task("tpnm.panic_task", func(ctx *Ctx) error {
		panic("boom")
	})

	wf, err := NewWorkflow("panic-no-middleware").
		Step("tpnm.panic_task").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Create engine with NO middleware
	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
	if result.Error == nil {
		t.Fatal("expected error to be set")
	}
	if !strings.Contains(result.Error.Error(), "panic") {
		t.Fatalf("expected panic in error message, got: %v", result.Error)
	}
}

// TestPerJobTokenRejection verifies the spawn server rejects messages with invalid or missing tokens.
func TestPerJobTokenRejection(t *testing.T) {
	// Create a spawnServer directly (no gRPC transport needed -- call methods in-process)
	ss := &spawnServer{
		jobs:   make(map[string]*SpawnJob),
		secret: "test-secret",
	}

	// Register a job with a known token
	ss.addJob(&SpawnJob{
		ID:       "job-1",
		TaskName: "test-task",
		token:    "valid-token-123",
		resultCh: make(chan *spawnResult, 1),
	})

	// Helper to create context with gRPC metadata
	ctxWithToken := func(jobToken string) context.Context {
		md := metadata.Pairs("x-gostage-job-token", jobToken)
		return metadata.NewIncomingContext(context.Background(), md)
	}
	ctxNoToken := func() context.Context {
		return metadata.NewIncomingContext(context.Background(), metadata.MD{})
	}

	// Test 1: Empty jobID -> InvalidArgument
	msg := &pb.IPCMessage{
		Type:    pb.MessageType_MESSAGE_TYPE_UNSPECIFIED,
		Context: &pb.MessageContext{SessionId: ""},
	}
	_, err := ss.SendMessage(ctxWithToken("valid-token-123"), msg)
	if err == nil {
		t.Fatal("expected error for empty jobID")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got: %v", err)
	}

	// Test 2: Valid jobID + wrong token -> PermissionDenied
	msg.Context.SessionId = "job-1"
	_, err = ss.SendMessage(ctxWithToken("wrong-token"), msg)
	if err == nil {
		t.Fatal("expected error for wrong token")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got: %v", err)
	}

	// Test 3: Valid jobID + no token -> PermissionDenied
	_, err = ss.SendMessage(ctxNoToken(), msg)
	if err == nil {
		t.Fatal("expected error for missing token")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got: %v", err)
	}

	// Test 4: Valid jobID + valid token -> success
	_, err = ss.SendMessage(ctxWithToken("valid-token-123"), msg)
	if err != nil {
		t.Fatalf("expected success with valid token, got: %v", err)
	}

	// Test 5: RequestWorkflowDefinition with wrong token -> PermissionDenied
	req := &pb.ReadySignal{ChildId: "job-1"}
	_, err = ss.RequestWorkflowDefinition(ctxWithToken("wrong-token"), req)
	if err == nil {
		t.Fatal("expected error for wrong token on RequestWorkflowDefinition")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got: %v", err)
	}

	// Test 6: RequestWorkflowDefinition with valid token -> success
	_, err = ss.RequestWorkflowDefinition(ctxWithToken("valid-token-123"), req)
	if err != nil {
		t.Fatalf("expected success with valid token, got: %v", err)
	}
}

// TestOnStepCompletePanicRecovery verifies that a panicking onStepComplete callback
// does not crash the worker or hang the run.
func TestOnStepCompletePanicRecovery(t *testing.T) {
	ResetTaskRegistry()

	Task("oscpr.task", func(ctx *Ctx) error {
		Set(ctx, "done", true)
		return nil
	})

	wf, err := NewWorkflow("callback-panic",
		OnStepComplete(func(stepName string, ctx *Ctx) {
			panic("callback boom")
		}),
	).
		Step("oscpr.task").
		Commit()
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
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}
	if result.Store["done"] != true {
		t.Fatal("expected done=true in store")
	}
}

// TestOnErrorPanicRecovery verifies that a panicking onError callback
// does not crash the worker or hang the run.
func TestOnErrorPanicRecovery(t *testing.T) {
	ResetTaskRegistry()

	Task("oepr.fail_task", func(ctx *Ctx) error {
		return fmt.Errorf("task failure")
	})

	wf, err := NewWorkflow("error-callback-panic",
		OnError(func(err error) {
			panic("error callback boom")
		}),
	).
		Step("oepr.fail_task").
		Commit()
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
	if result.Status != Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
	if result.Error == nil {
		t.Fatal("expected error to be set")
	}
}

func TestWaitAfterCompletion(t *testing.T) {
	ResetTaskRegistry()

	Task("d8.wait_done", func(ctx *Ctx) error {
		Set(ctx, "done", true)
		return nil
	})

	wf, _ := NewWorkflow("wait-done").Step("d8.wait_done").Commit()

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// RunSync completes synchronously -- run is never in e.runs
	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}

	// Wait should hit the persistence fallback path
	waitResult, err := engine.Wait(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("Wait after completion failed: %v", err)
	}
	if waitResult.Status != Completed {
		t.Fatalf("expected Completed from Wait, got %s", waitResult.Status)
	}
}

func TestEngine_DeleteRun(t *testing.T) {
	ResetTaskRegistry()

	Task("d8.del_task", func(ctx *Ctx) error {
		Set(ctx, "value", "hello")
		return nil
	})

	wf, _ := NewWorkflow("del-run").Step("d8.del_task").Commit()

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()

	result, err := engine.RunSync(ctx, wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}

	// Delete the run
	if err := engine.DeleteRun(ctx, result.RunID); err != nil {
		t.Fatalf("DeleteRun failed: %v", err)
	}

	// LoadRun should return RunNotFoundError
	_, err = engine.persistence.LoadRun(ctx, result.RunID)
	var notFound *RunNotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("expected RunNotFoundError after delete, got %v", err)
	}
}

func TestDeleteRunWaitsForCompletion(t *testing.T) {
	ResetTaskRegistry()

	blockCh := make(chan struct{})
	startedCh := make(chan struct{})

	Task("d9.blocking_task", func(ctx *Ctx) error {
		close(startedCh)  // signal that the task has started
		<-blockCh         // block until test unblocks us
		Set(ctx, "done", true)
		return nil
	})

	wf, _ := NewWorkflow("delete-wait").Step("d9.blocking_task").Commit()

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()

	// Start async run
	runID, err := engine.Run(ctx, wf, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for the task to start executing
	<-startedCh

	// Call DeleteRun in a goroutine -- it should block until the task completes
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- engine.DeleteRun(ctx, runID)
	}()

	// Give DeleteRun a moment to start waiting
	select {
	case <-deleteDone:
		t.Fatal("DeleteRun returned before task completed -- should have waited")
	case <-time.After(50 * time.Millisecond):
		// Good -- DeleteRun is blocking as expected
	}

	// Unblock the task
	close(blockCh)

	// Now DeleteRun should complete
	select {
	case err := <-deleteDone:
		if err != nil {
			t.Fatalf("DeleteRun failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("DeleteRun did not complete after task finished")
	}

	// Verify persistence is clean
	_, err = engine.persistence.LoadRun(ctx, runID)
	var nf *RunNotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("expected RunNotFoundError after delete, got %v", err)
	}
}

// === SQLite schema -- no checkpoints table (Issue 10) ===

func TestSQLite_NoCheckpointsTable(t *testing.T) {
	engine, err := New(WithSQLite(t.TempDir() + "/nochk.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// Query sqlite_master for a table named "checkpoints".
	var name string
	row := engine.persistence.(*sqlitePersistence).db.QueryRowContext(
		context.Background(),
		`SELECT name FROM sqlite_master WHERE type='table' AND name='checkpoints'`,
	)
	err = row.Scan(&name)
	if err == nil {
		t.Fatal("checkpoints table must not exist in new databases")
	}
	// sql.ErrNoRows is the expected outcome.
}

// === IPC uses dedicated MESSAGE_TYPE_IPC (Issue 11) ===

func TestIPC_UsesCorrectProtoType(t *testing.T) {
	// Verify that Send() from a parent-process task routes through dispatchMessage
	// without needing __ipc__ sentinel -- the local path in Send() calls
	// engine.dispatchMessage directly. In child processes, the new proto type is used.
	// This test exercises the local (parent-process) IPC routing.
	ResetTaskRegistry()

	var received bool
	Task("ipc.proto.task", func(ctx *Ctx) error {
		return Send(ctx, "ipc_proto_evt", Params{"ok": true})
	})

	wf, err := NewWorkflow("ipc-proto-test").Step("ipc.proto.task").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	engine.OnMessage("ipc_proto_evt", func(msgType string, payload map[string]any) {
		if payload["ok"] == true {
			received = true
		}
	})

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
	if !received {
		t.Fatal("IPC message was not received by handler")
	}
}

// === Test helpers ===

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
