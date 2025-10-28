package e2e

import (
    "context"
    "errors"
    "fmt"
    "sync/atomic"
    "testing"
    "time"

	"github.com/davidroman0O/gostage/v3"
	"github.com/davidroman0O/gostage/v3/e2e/testkit"
	rt "github.com/davidroman0O/gostage/v3/runtime"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/store"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/davidroman0O/gostage/v3/workflow"
)

func TestCoreLifecycleNodeAPI(t *testing.T) {
	testkit.ResetRegistry(t)

	gostage.MustRegisterAction("core.progress", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			if broker := ctx.Broker(); broker != nil {
				_ = broker.Progress(50, "halfway")
			}
			_ = gostage.EmitActionEvent(ctx, "action.custom", "halfway", map[string]any{"from": "test"})
			_ = store.Put(ctx, "executed", true)
			return nil
		}
	})

	def := workflow.Definition{
		Name: "Node API",
		Tags: []string{"primary"},
		Stages: []workflow.Stage{
			{
				Name: "stage",
				Actions: []workflow.Action{
					{Ref: "core.progress"},
				},
			},
		},
	}
	workflowID, _ := gostage.MustRegisterWorkflow(def)

	backends := testkit.NewMemoryBackends()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeOpts := append(testkit.MemoryOptions(backends), gostage.WithPool(gostage.PoolConfig{Name: "primary", Tags: []string{"primary"}, Slots: 2}))
	node, diag, err := gostage.Run(ctx, nodeOpts...)
	if err != nil {
		t.Fatalf("run error: %v", err)
	}

	collector := testkit.StartDiagnosticsCollector(t, diag)
	t.Cleanup(collector.Close)
	t.Cleanup(func() { _ = node.Close() })

	telemetryBuf := testkit.StartTelemetryBuffer(ctx, t, node, 128)
	t.Cleanup(telemetryBuf.Close)

	healthBuf := testkit.StartHealthBuffer(ctx, t, node, 32)
	t.Cleanup(healthBuf.Close)

	initialStore := map[string]any{"init": true}
	runID, err := node.Submit(ctx,
		gostage.WorkflowRef(workflowID),
		gostage.WithPriority(5),
		gostage.WithTags("extra"),
		gostage.WithInitialStore(initialStore),
		gostage.WithMetadata(map[string]any{"request": "alpha"}),
	)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	waitCtx, cancelWait := context.WithTimeout(ctx, 15*time.Second)
	defer cancelWait()
	result, err := node.Wait(waitCtx, runID)
	if err != nil {
		for _, evt := range collector.Events() {
			t.Logf("diagnostic: component=%s severity=%s err=%v", evt.Component, evt.Severity, evt.Err)
		}
		t.Fatalf("wait: %v", err)
	}
	if !result.Success {
		t.Fatalf("workflow failed: %+v", result)
	}
	if ok, _ := result.Output["executed"].(bool); !ok {
		t.Fatalf("expected store value set, got %+v", result.Output)
	}
	if result.Attempt != 1 {
		t.Fatalf("expected attempt=1, got %d", result.Attempt)
	}
	if result.Reason != gostage.TerminationReasonSuccess {
		t.Fatalf("expected success reason, got %s", result.Reason)
	}

	stats, err := node.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Completed != 1 || stats.Failed != 0 || stats.Cancelled != 0 {
		t.Fatalf("unexpected stats %+v", stats)
	}
	if len(stats.Pools) == 0 {
		t.Fatalf("expected pool snapshot")
	}

	statsCtx, cancelStats := context.WithTimeout(ctx, time.Second)
	defer cancelStats()
	stats2, err := node.StatsWithContext(statsCtx)
	if err != nil {
		t.Fatalf("stats with context: %v", err)
	}
	if stats2.Completed != stats.Completed || stats2.Failed != stats.Failed || stats2.Cancelled != stats.Cancelled {
		t.Fatalf("stats mismatch %+v vs %+v", stats, stats2)
	}

	summaryEvent := telemetryBuf.Next(t, telemetry.EventWorkflowSummary, 5*time.Second)
	if success, ok := summaryEvent.Metadata["success"].(bool); !ok || !success {
		t.Fatalf("expected success telemetry, got %+v", summaryEvent.Metadata)
	}
	healthEvt := healthBuf.Next(t, "healthy", 2*time.Second)
	if healthEvt.Pool == "" {
		t.Fatalf("expected pool name in health event")
	}

	if node.State == nil {
		t.Fatalf("expected state facade")
	}
	summary := testkit.AwaitWorkflowSummaryWithReason(t, node.State, ctx, runID, state.TerminationReasonSuccess)
	if summary.State != state.WorkflowCompleted {
		t.Fatalf("expected completed state, got %s", summary.State)
	}
	if summary.TerminationReason != state.TerminationReasonSuccess {
		t.Fatalf("expected summary reason success, got %s", summary.TerminationReason)
	}
	if _, ok := summary.Metadata["gostage.initial_store"]; ok {
		t.Fatalf("initial store leaked into workflow metadata: %+v", summary.Metadata)
	}

	list, err := node.State.ListWorkflows(ctx, state.StateFilter{States: []state.WorkflowState{state.WorkflowCompleted}, Tags: []string{"primary"}, Limit: 5})
	if err != nil {
		t.Fatalf("list workflows: %v", err)
	}
	if len(list) == 0 {
		t.Fatalf("expected workflow listing")
	}
	if _, ok := list[0].Metadata["gostage.initial_store"]; ok {
		t.Fatalf("initial store leaked into list metadata: %+v", list[0].Metadata)
	}

	history, err := node.State.ActionHistory(ctx, runID)
	if err != nil {
		t.Fatalf("action history: %v", err)
	}
	if len(history) == 0 {
		t.Fatalf("expected action history entries")
	}
	var foundProgress bool
	for _, rec := range history {
		if rec.Progress == 50 && rec.Message == "halfway" {
			foundProgress = true
			break
		}
	}
	if !foundProgress {
		t.Fatalf("expected progress entry in history, got %+v", history)
	}

	snap := backends.Observer.Snapshot()
	if len(snap.ActionEvents) == 0 {
		t.Fatalf("expected custom action events in observer snapshot")
	}
	custom := snap.ActionEvents[0]
	if custom.Kind != "action.custom" || custom.Message != "halfway" {
		t.Fatalf("unexpected custom event %+v", custom)
	}
	if custom.Metadata == nil || custom.Metadata["from"] != "test" {
		t.Fatalf("unexpected custom metadata %+v", custom.Metadata)
	}
}

func TestCoreLifecycleStatsCountersWithRetry(t *testing.T) {
	testkit.ResetRegistry(t)

var runs atomic.Int32
gostage.MustRegisterAction("test.retry", func() gostage.ActionFunc {
    return func(ctx rt.Context) error {
        wfStore := ctx.Workflow().Store()
        if wfStore.IsZero() {
            wfStore = ctx.Store()
        }
        if _, err := store.Get[string](wfStore, "info"); err != nil {
            return fmt.Errorf("missing initial info: %w", err)
        }
        count, err := store.Get[int](wfStore, "count")
        if err != nil {
            count = 0
        }
        count++
        if err := store.Put(wfStore, "count", count); err != nil {
            return err
        }
        attempt := runs.Add(1)
        if attempt == 1 {
            return errors.New("transient failure")
        }
        return nil
    }
})

	def := workflow.Definition{
		Name: "Retryable",
		Stages: []workflow.Stage{{
			Name:    "stage",
			Actions: []workflow.Action{{Ref: "test.retry"}},
		}},
	}
	workflowID, _ := gostage.MustRegisterWorkflow(def)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	policy := gostage.FailurePolicyFunc(func(_ context.Context, info gostage.FailureContext) gostage.FailureOutcome {
		if info.Attempt < 2 {
			return gostage.RetryOutcome()
		}
		return gostage.AckOutcome()
	})

	node, diag, err := gostage.Run(ctx, gostage.WithFailurePolicy(policy))
	if err != nil {
		t.Fatalf("run error: %v", err)
	}
	t.Cleanup(func() { _ = node.Close() })
	go func() {
		for range diag {
		}
	}()

runID, err := node.Submit(ctx, gostage.WorkflowRef(workflowID), gostage.WithInitialStore(map[string]any{"info": "seed"}))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	waitCtx, cancelWait := context.WithTimeout(ctx, 5*time.Second)
	defer cancelWait()
	result, err := node.Wait(waitCtx, runID)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
if !result.Success {
    t.Fatalf("expected success after retry, got %+v", result)
}
if result.Attempt != 2 {
    t.Fatalf("expected attempt=2, got %d", result.Attempt)
}
if result.Reason != gostage.TerminationReasonSuccess {
    t.Fatalf("expected success reason, got %s", result.Reason)
}
if seed, ok := result.Output["info"].(string); !ok || seed != "seed" {
    t.Fatalf("expected seed retained in final store, got %+v", result.Output["info"])
}
coerceInt := func(val any) (int, bool) {
    switch v := val.(type) {
    case int:
        return v, true
    case int32:
        return int(v), true
    case int64:
        return int(v), true
    case float64:
        return int(v), true
    default:
        return 0, false
    }
}
if count, ok := coerceInt(result.Output["count"]); !ok || count != 2 {
    t.Fatalf("expected count=2 in final store, got %+v", result.Output["count"])
}

	snapshot, err := node.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if snapshot.Completed != 1 || snapshot.Failed != 0 || snapshot.Cancelled != 0 {
		t.Fatalf("unexpected counters %+v", snapshot)
	}
}

func TestCoreLifecycleStatsCountersFailure(t *testing.T) {
	testkit.ResetRegistry(t)

	gostage.MustRegisterAction("test.fail", func() gostage.ActionFunc {
		return func(rt.Context) error {
			return errors.New("boom")
		}
	})

	def := workflow.Definition{
		Name: "Failure",
		Stages: []workflow.Stage{{
			Name:    "stage",
			Actions: []workflow.Action{{Ref: "test.fail"}},
		}},
	}
	workflowID, _ := gostage.MustRegisterWorkflow(def)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, diag, err := gostage.Run(ctx)
	if err != nil {
		t.Fatalf("run error: %v", err)
	}
	t.Cleanup(func() { _ = node.Close() })
	go func() {
		for range diag {
		}
	}()

	runID, err := node.Submit(ctx, gostage.WorkflowRef(workflowID))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	waitCtx, cancelWait := context.WithTimeout(ctx, 5*time.Second)
	defer cancelWait()
	result, err := node.Wait(waitCtx, runID)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if result.Success {
		t.Fatalf("expected failure result, got %+v", result)
	}
	if result.Error == nil {
		t.Fatalf("expected error in failure result")
	}
	if result.Attempt != 1 {
		t.Fatalf("expected attempt=1, got %d", result.Attempt)
	}
	if result.Reason != gostage.TerminationReasonFailure {
		t.Fatalf("expected failure reason, got %s", result.Reason)
	}

	snapshot, err := node.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if snapshot.Completed != 0 || snapshot.Failed != 1 || snapshot.Cancelled != 0 {
		t.Fatalf("unexpected counters %+v", snapshot)
	}
}

func TestStateFacadeListsRunningWorkflow(t *testing.T) {
	testkit.ResetRegistry(t)

	blocker := make(chan struct{})
	gostage.MustRegisterAction("state.running.block", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-blocker:
				return nil
			}
		}
	})

	def := workflow.Definition{
		Name: "RunningState",
		Stages: []workflow.Stage{{
			Name:    "blocking-stage",
			Actions: []workflow.Action{{Ref: "state.running.block"}},
		}},
	}
	workflowID, _ := gostage.MustRegisterWorkflow(def)

	backends := testkit.NewMemoryBackends()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, diagCh, err := gostage.Run(ctx, testkit.MemoryOptions(backends)...)
	if err != nil {
		t.Fatalf("run error: %v", err)
	}
	diag := testkit.StartDiagnosticsCollector(t, diagCh)
	t.Cleanup(func() {
		if t.Failed() {
			for _, evt := range diag.Events() {
				t.Logf("diagnostic: component=%s severity=%s err=%v metadata=%v", evt.Component, evt.Severity, evt.Err, evt.Metadata)
			}
		}
		diag.Close()
	})
	t.Cleanup(func() { _ = node.Close() })

	if node.State == nil {
		t.Fatalf("expected state reader on node")
	}

	runID, err := node.Submit(ctx, gostage.WorkflowRef(workflowID))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	claimDeadline := time.Now().Add(5 * time.Second)
	for {
		stats, err := node.Stats()
		if err != nil {
			t.Fatalf("stats while waiting for claim: %v", err)
		}
		if stats.InFlight > 0 {
			break
		}
		if time.Now().After(claimDeadline) {
			t.Fatalf("workflow not claimed in time")
		}
		time.Sleep(25 * time.Millisecond)
	}

	summary := testkit.WaitForWorkflowInState(t, node.State, state.WorkflowID(runID), state.WorkflowRunning)
	if summary.ID != state.WorkflowID(runID) {
		t.Fatalf("unexpected workflow id listed: got %s want %s", summary.ID, runID)
	}

	close(blocker)

	waitCtx, cancelWait := context.WithTimeout(ctx, 10*time.Second)
	defer cancelWait()
	result, err := node.Wait(waitCtx, runID)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if !result.Success {
		t.Fatalf("workflow failed: %+v", result)
	}

	checkCtx, cancelCheck := context.WithTimeout(ctx, time.Second)
	runningList, err := node.State.ListWorkflows(checkCtx, state.StateFilter{States: []state.WorkflowState{state.WorkflowRunning}})
	cancelCheck()
	if err != nil {
		t.Fatalf("list running workflows after completion: %v", err)
	}
	for _, entry := range runningList {
		if entry.ID == state.WorkflowID(runID) {
			t.Fatalf("workflow %s remained in running list after completion", runID)
		}
	}
}
