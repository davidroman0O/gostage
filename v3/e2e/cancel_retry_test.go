package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3"
	"github.com/davidroman0O/gostage/v3/e2e/testkit"
	rt "github.com/davidroman0O/gostage/v3/runtime"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/davidroman0O/gostage/v3/workflow"
)

func TestCancellationQueuedWorkflow(t *testing.T) {
	testkit.ResetRegistry(t)

	gostage.MustRegisterAction("cancel.noop", func() gostage.ActionFunc {
		return func(rt.Context) error { return nil }
	})

	def := workflow.Definition{
		Name: "QueuedCancel",
		Stages: []workflow.Stage{{
			Name:    "stage",
			Actions: []workflow.Action{{Ref: "cancel.noop"}},
		}},
	}
	workflowID, _ := gostage.MustRegisterWorkflow(def)

	backends := testkit.NewMemoryBackends()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := append(testkit.MemoryOptions(backends),
		gostage.WithPool(gostage.PoolConfig{Name: "primary", Tags: []string{"primary"}, Slots: 1}),
		gostage.WithDispatcher(gostage.DispatcherConfig{ClaimInterval: 500 * time.Millisecond}),
	)
	node, diag, err := gostage.Run(ctx, opts...)
	if err != nil {
		t.Fatalf("run error: %v", err)
	}
	collector := testkit.StartDiagnosticsCollector(t, diag)
	t.Cleanup(collector.Close)
	t.Cleanup(func() { _ = node.Close() })

	teleBuf := testkit.StartTelemetryBuffer(ctx, t, node, 32)
	t.Cleanup(teleBuf.Close)
	healthBuf := testkit.StartHealthBuffer(ctx, t, node, 32)
	t.Cleanup(healthBuf.Close)

	runID, err := node.Submit(ctx, gostage.WorkflowRef(workflowID), gostage.WithTags("primary"))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	if err := node.Cancel(ctx, runID); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		stats, err := node.Stats()
		if err != nil {
			t.Fatalf("stats: %v", err)
		}
		if stats.QueueDepth == 0 && stats.InFlight == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("queue did not drain after cancellation; snapshot=%+v", stats)
		}
		time.Sleep(25 * time.Millisecond)
	}

	if node.State == nil {
		t.Fatalf("expected state facade initialised")
	}
	lookupCtx, cancelLookup := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancelLookup()
	if summary, err := node.State.WorkflowSummary(lookupCtx, runID); err == nil {
		if summary.State != state.WorkflowCancelled && summary.State != state.WorkflowCompleted {
			t.Fatalf("unexpected workflow state for queued cancel: %s", summary.State)
		}
	} else {
		t.Logf("workflow summary unavailable after queued cancel: %v", err)
	}

	cancelEvent := teleBuf.Next(t, telemetry.EventWorkflowCancelRequest, 3*time.Second)
	if pending, _ := cancelEvent.Metadata["pending"].(bool); !pending {
		t.Fatalf("expected cancel request metadata to mark pending queue entry, got %+v", cancelEvent.Metadata)
	}
}

func TestCancellationInFlightWorkflow(t *testing.T) {
	testkit.ResetRegistry(t)

	gostage.MustRegisterAction("cancel.block", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Second):
				return nil
			}
		}
	})

	def := workflow.Definition{
		Name: "Cancelable",
		Stages: []workflow.Stage{{
			Name:    "block",
			Actions: []workflow.Action{{Ref: "cancel.block"}},
		}},
	}
	workflowID, _ := gostage.MustRegisterWorkflow(def)

	backends := testkit.NewMemoryBackends()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, diag, err := gostage.Run(ctx, testkit.MemoryOptions(backends)...)
	if err != nil {
		t.Fatalf("run error: %v", err)
	}
	collector := testkit.StartDiagnosticsCollector(t, diag)
	t.Cleanup(collector.Close)
	t.Cleanup(func() { _ = node.Close() })

	telemetryBuf := testkit.StartTelemetryBuffer(ctx, t, node, 32)
	t.Cleanup(telemetryBuf.Close)

	runID, err := node.Submit(ctx, gostage.WorkflowRef(workflowID))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	claimDeadline := time.Now().Add(5 * time.Second)
	for {
		snap, err := node.Stats()
		if err != nil {
			t.Fatalf("stats while waiting for claim: %v", err)
		}
		if snap.InFlight > 0 {
			break
		}
		if time.Now().After(claimDeadline) {
			t.Fatalf("workflow not claimed in time")
		}
		time.Sleep(50 * time.Millisecond)
	}

	if err := node.Cancel(ctx, runID); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	waitCtx, cancelWait := context.WithTimeout(ctx, 15*time.Second)
	defer cancelWait()
	res, err := node.Wait(waitCtx, runID)
	if err != nil {
		t.Fatalf("wait after cancel: %v", err)
	}
	if res.Success {
		t.Fatalf("expected cancellation result, got %+v", res)
	}
	if res.Error == nil || res.Error.Error() != context.Canceled.Error() {
		t.Fatalf("expected context canceled error, got %+v", res.Error)
	}
	if res.Reason != gostage.TerminationReasonUserCancel {
		t.Fatalf("expected user cancel reason, got %s", res.Reason)
	}

	snapAfter, err := node.Stats()
	if err != nil {
		t.Fatalf("stats after cancellation: %v", err)
	}
	if snapAfter.Cancelled != 1 {
		t.Fatalf("expected cancelled count to be 1, got %+v", snapAfter)
	}

	evt := telemetryBuf.Next(t, telemetry.EventWorkflowSummary, 2*time.Second)
	if success, ok := evt.Metadata["success"].(bool); ok && success {
		t.Fatalf("expected unsuccessful summary telemetry, got %+v", evt.Metadata)
	}
	summary := testkit.AwaitWorkflowSummaryWithReason(t, node.State, ctx, runID, state.TerminationReasonUserCancel)
	if summary.State != state.WorkflowCancelled {
		t.Fatalf("expected cancelled state, got %s", summary.State)
	}
	if summary.TerminationReason != state.TerminationReasonUserCancel {
		t.Fatalf("expected summary reason user_cancel, got %s", summary.TerminationReason)
	}
	if snap := backends.Observer.Snapshot(); snap.Summaries != nil {
		if obs, ok := snap.Summaries[runID]; ok && obs.Reason != state.TerminationReasonUserCancel {
			t.Fatalf("observer summary reason mismatch: got %s", obs.Reason)
		}
	}

	if len(summary.Stages) == 0 {
		t.Fatalf("expected stage entries in summary, got none")
	}
	stageCancelled := false
	actionCancelled := false
	for _, stage := range summary.Stages {
		if stage == nil {
			continue
		}
		if stage.Status == state.WorkflowCancelled {
			stageCancelled = true
		}
		if len(stage.Actions) == 0 {
			continue
		}
		for _, action := range stage.Actions {
			if action == nil {
				continue
			}
			if action.Status == state.WorkflowCancelled {
				actionCancelled = true
			}
		}
	}
	if !stageCancelled {
		t.Fatalf("expected at least one cancelled stage, got %+v", summary.Stages)
	}
	if !actionCancelled {
		t.Fatalf("expected at least one cancelled action, got %+v", summary.Stages)
	}

	snap := backends.Observer.Snapshot()
	stageEventCancelled := false
	for _, evt := range snap.StageStatuses {
		if evt.WorkflowID == string(runID) && evt.Status == state.WorkflowCancelled {
			stageEventCancelled = true
			break
		}
	}
	if !stageEventCancelled {
		t.Fatalf("observer did not emit cancelled stage status: %+v", snap.StageStatuses)
	}
	actionEventCancelled := false
	for _, evt := range snap.ActionStatuses {
		if evt.WorkflowID == string(runID) && evt.Status == state.WorkflowCancelled {
			actionEventCancelled = true
			break
		}
	}
	if !actionEventCancelled {
		t.Fatalf("observer did not emit cancelled action status: %+v", snap.ActionStatuses)
	}
}

func TestRetryPolicyUpdatesSummary(t *testing.T) {
	testkit.ResetRegistry(t)

	var runs int
	gostage.MustRegisterAction("retry.once", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			runs++
			if runs == 1 {
				return errors.New("transient failure")
			}
			_ = gostage.EmitActionEvent(ctx, "action.retry", "recovered", nil)
			return nil
		}
	})

	def := workflow.Definition{
		Name: "RetrySummary",
		Stages: []workflow.Stage{{
			Name:    "stage",
			Actions: []workflow.Action{{Ref: "retry.once"}},
		}},
	}
	workflowID, _ := gostage.MustRegisterWorkflow(def)

	backends := testkit.NewMemoryBackends()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	policy := gostage.FailurePolicyFunc(func(_ context.Context, info gostage.FailureContext) gostage.FailureOutcome {
		if info.Attempt < 2 {
			return gostage.RetryOutcome()
		}
		return gostage.AckOutcome()
	})

	node, diag, err := gostage.Run(ctx, append(testkit.MemoryOptions(backends), gostage.WithFailurePolicy(policy))...)
	if err != nil {
		t.Fatalf("run error: %v", err)
	}
	collector := testkit.StartDiagnosticsCollector(t, diag)
	t.Cleanup(collector.Close)
	t.Cleanup(func() { _ = node.Close() })

	telemetryBuf := testkit.StartTelemetryBuffer(ctx, t, node, 32)
	t.Cleanup(telemetryBuf.Close)

	runID, err := node.Submit(ctx, gostage.WorkflowRef(workflowID))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	waitCtx, cancelWait := context.WithTimeout(ctx, 5*time.Second)
	defer cancelWait()
	result, err := node.Wait(waitCtx, runID)
	if err != nil {
		for _, evt := range collector.Events() {
			t.Logf("diag: component=%s severity=%s err=%v", evt.Component, evt.Severity, evt.Err)
		}
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

	summaryEvent := telemetryBuf.Next(t, telemetry.EventWorkflowSummary, 3*time.Second)
	if success, _ := summaryEvent.Metadata["success"].(bool); !success {
		t.Fatalf("expected summary success, got %+v", summaryEvent.Metadata)
	}
	stats, err := node.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Completed != 1 || stats.Failed != 0 || stats.Cancelled != 0 {
		t.Fatalf("unexpected counters %+v", stats)
	}

	summary := testkit.AwaitWorkflowSummaryWithReason(t, node.State, ctx, runID, state.TerminationReasonSuccess)
	if summary.State != state.WorkflowCompleted {
		t.Fatalf("expected completed state, got %s", summary.State)
	}
	if summary.TerminationReason != state.TerminationReasonSuccess {
		t.Fatalf("expected summary reason success, got %s", summary.TerminationReason)
	}
	snap := backends.Observer.Snapshot()
	if sum, ok := snap.Summaries[runID]; !ok || sum.Attempt != 2 {
		t.Fatalf("expected capture summary attempt=2, got %+v", sum)
	}
}

func TestFailurePolicyCancelSetsReason(t *testing.T) {
	testkit.ResetRegistry(t)

	gostage.MustRegisterAction("policy.fail", func() gostage.ActionFunc {
		return func(rt.Context) error {
			return errors.New("hard failure")
		}
	})

	def := workflow.Definition{
		Name: "PolicyCancel",
		Stages: []workflow.Stage{{
			Name:    "stage",
			Actions: []workflow.Action{{Ref: "policy.fail"}},
		}},
	}
	workflowID, _ := gostage.MustRegisterWorkflow(def)

	backends := testkit.NewMemoryBackends()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	policy := gostage.FailurePolicyFunc(func(_ context.Context, info gostage.FailureContext) gostage.FailureOutcome {
		if info.Attempt > 1 {
			t.Fatalf("unexpected retry attempt %d", info.Attempt)
		}
		return gostage.CancelOutcome(state.TerminationReasonPolicyCancel)
	})

	node, diag, err := gostage.Run(ctx, append(testkit.MemoryOptions(backends), gostage.WithFailurePolicy(policy))...)
	if err != nil {
		t.Fatalf("run error: %v", err)
	}
	collector := testkit.StartDiagnosticsCollector(t, diag)
	t.Cleanup(collector.Close)
	t.Cleanup(func() { _ = node.Close() })

	telemetryBuf := testkit.StartTelemetryBuffer(ctx, t, node, 16)
	t.Cleanup(telemetryBuf.Close)

	runID, err := node.Submit(ctx, gostage.WorkflowRef(workflowID))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	waitCtx, cancelWait := context.WithTimeout(ctx, 5*time.Second)
	defer cancelWait()
	result, err := node.Wait(waitCtx, runID)
	if err != nil {
		for _, evt := range collector.Events() {
			t.Logf("diag: component=%s severity=%s err=%v", evt.Component, evt.Severity, evt.Err)
		}
		t.Fatalf("wait: %v", err)
	}
	if result.Success {
		t.Fatalf("expected failure result, got %+v", result)
	}
	if result.Error == nil {
		t.Fatalf("expected error in result")
	}
	if result.Reason != gostage.TerminationReasonPolicyCancel {
		t.Fatalf("expected policy cancel reason, got %s", result.Reason)
	}

	summaryEvent := telemetryBuf.Next(t, telemetry.EventWorkflowSummary, 3*time.Second)
	if success, _ := summaryEvent.Metadata["success"].(bool); success {
		t.Fatalf("expected unsuccessful summary metadata, got %+v", summaryEvent.Metadata)
	}
	if snap := backends.Observer.Snapshot(); snap.Workflows != nil {
		if wf, ok := snap.Workflows[runID]; ok && wf.State != state.WorkflowCancelled {
			t.Fatalf("observer workflow state mismatch: %s", wf.State)
		}
	}
	summary := testkit.AwaitWorkflowSummaryWithReason(t, node.State, ctx, runID, state.TerminationReasonPolicyCancel)
	if summary.State != state.WorkflowCancelled {
		t.Fatalf("expected cancelled state, got %s", summary.State)
	}
	if summary.TerminationReason != state.TerminationReasonPolicyCancel {
		t.Fatalf("expected summary reason policy_cancel, got %s", summary.TerminationReason)
	}
}
