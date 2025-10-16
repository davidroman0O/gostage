package e2e

import (
	"context"
	"errors"
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
	summary := testkit.AwaitWorkflowSummary(t, node.State, backends.Observer, ctx, runID)
	if summary.State != state.WorkflowCompleted {
		t.Fatalf("expected completed state, got %s", summary.State)
	}

	list, err := node.State.ListWorkflows(ctx, state.StateFilter{States: []state.WorkflowState{state.WorkflowCompleted}, Tags: []string{"primary"}, Limit: 5})
	if err != nil {
		t.Fatalf("list workflows: %v", err)
	}
	if len(list) == 0 {
		t.Fatalf("expected workflow listing")
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
			if runs.Add(1) == 1 {
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

	policy := gostage.FailurePolicyFunc(func(_ context.Context, info gostage.FailureContext) gostage.FailureDecision {
		if info.Attempt < 2 {
			return gostage.FailureDecisionRetry
		}
		return gostage.FailureDecisionAck
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
	if !result.Success {
		t.Fatalf("expected success after retry, got %+v", result)
	}
	if result.Attempt != 2 {
		t.Fatalf("expected attempt=2, got %d", result.Attempt)
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
	if result.Attempt != 1 {
		t.Fatalf("expected attempt=1, got %d", result.Attempt)
	}

	snapshot, err := node.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if snapshot.Completed != 0 || snapshot.Failed != 1 || snapshot.Cancelled != 0 {
		t.Fatalf("unexpected counters %+v", snapshot)
	}
}
