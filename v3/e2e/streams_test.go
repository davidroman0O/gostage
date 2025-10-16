package e2e

import (
	"bytes"
	"context"
	"errors"
	"log"
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

func TestTelemetryStreamsWithLoggerSink(t *testing.T) {
	testkit.ResetRegistry(t)

	gostage.MustRegisterAction("streams.progress", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			if broker := ctx.Broker(); broker != nil {
				_ = broker.Progress(40, "processing")
				_ = broker.Progress(100, "done")
			}
			_ = gostage.EmitActionEvent(ctx, "action.custom", "halfway", map[string]any{"source": "test"})
			_ = store.Put(ctx, "value", 42)
			return nil
		}
	})

	def := workflow.Definition{
		Name: "TelemetryStreams",
		Tags: []string{"primary"},
		Stages: []workflow.Stage{{
			Name:    "stage",
			Actions: []workflow.Action{{Ref: "streams.progress"}},
		}},
	}
	workflowID, _ := gostage.MustRegisterWorkflow(def)

	backends := testkit.NewMemoryBackends()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var unwanted atomic.Int32
	var unwantedHealth atomic.Int32

	logBuf := &bytes.Buffer{}
	logger := log.New(logBuf, "telemetry:", 0)

	opts := append(testkit.MemoryOptions(backends),
		gostage.WithTelemetrySink(telemetry.NewLoggerSink(logger)),
		gostage.WithPool(gostage.PoolConfig{Name: "primary", Tags: []string{"primary"}, Slots: 1}),
	)

	node, diag, err := gostage.Run(ctx, opts...)
	if err != nil {
		t.Fatalf("run error: %v", err)
	}
	collector := testkit.StartDiagnosticsCollector(t, diag)
	t.Cleanup(collector.Close)
	t.Cleanup(func() { _ = node.Close() })

	cancelStream := node.StreamTelemetry(ctx, func(telemetry.Event) {
		unwanted.Add(1)
	})
	cancelStream()

	cancelHealth := node.StreamHealth(ctx, func(gostage.HealthEvent) {
		unwantedHealth.Add(1)
	})
	cancelHealth()

	teleBuf := testkit.StartTelemetryBuffer(ctx, t, node, 128)
	t.Cleanup(teleBuf.Close)
	healthBuf := testkit.StartHealthBuffer(ctx, t, node, 32)
	t.Cleanup(healthBuf.Close)

	runID, err := node.Submit(ctx,
		gostage.WorkflowRef(workflowID),
		gostage.WithTags("primary"),
		gostage.WithMetadata(map[string]any{"request": "telemetry"}),
	)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	waitCtx, cancelWait := context.WithTimeout(ctx, 10*time.Second)
	defer cancelWait()
	result, err := node.Wait(waitCtx, runID)
	if err != nil {
		for _, evt := range collector.Events() {
			t.Logf("diag: component=%s severity=%s err=%v", evt.Component, evt.Severity, evt.Err)
		}
		t.Fatalf("wait: %v", err)
	}
	if !result.Success {
		t.Fatalf("expected success result, got %+v", result)
	}
	if v, ok := result.Output["value"].(int); !ok || v != 42 {
		t.Fatalf("expected store value 42, got %+v", result.Output)
	}

	progress := teleBuf.Next(t, telemetry.EventActionProgressKind, 3*time.Second)
	if progress.Progress == nil || progress.Progress.Percent != 40 {
		t.Fatalf("expected progress 40, got %+v", progress.Progress)
	}
	summary := teleBuf.Next(t, telemetry.EventWorkflowSummary, 3*time.Second)
	if success, _ := summary.Metadata["success"].(bool); !success {
		t.Fatalf("expected success summary, got %+v", summary.Metadata)
	}

	readyEvt := healthBuf.Next(t, "healthy", 3*time.Second)
	if readyEvt.Pool != "primary" {
		t.Fatalf("expected pool 'primary', got %+v", readyEvt.Pool)
	}

	if unwanted.Load() != 0 {
		t.Fatalf("unexpected telemetry events delivered after cancel: %d", unwanted.Load())
	}
	if unwantedHealth.Load() != 0 {
		t.Fatalf("unexpected health events delivered after cancel: %d", unwantedHealth.Load())
	}

	if node.State == nil {
		t.Fatalf("expected state facade")
	}
	summaryState := testkit.AwaitWorkflowSummary(t, node.State, backends.Observer, ctx, runID)
	if summaryState.State != state.WorkflowCompleted {
		t.Fatalf("expected completed state, got %s", summaryState.State)
	}

	if logBuf.Len() == 0 {
		t.Fatalf("expected logger sink to record events")
	}
	if !bytes.Contains(logBuf.Bytes(), []byte("telemetry")) {
		t.Fatalf("expected telemetry prefix in logs, got %s", logBuf.String())
	}
}

func TestHealthStreamDegradationAndRecovery(t *testing.T) {
	testkit.ResetRegistry(t)

	var runs atomic.Int32
	gostage.MustRegisterAction("streams.failonce", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			if runs.Add(1) == 1 {
				return errors.New("boom")
			}
			if broker := ctx.Broker(); broker != nil {
				_ = broker.Progress(100, "recovered")
			}
			return nil
		}
	})

	def := workflow.Definition{
		Name: "HealthRecovery",
		Tags: []string{"primary"},
		Stages: []workflow.Stage{{
			Name:    "stage",
			Actions: []workflow.Action{{Ref: "streams.failonce"}},
		}},
	}
	workflowID, _ := gostage.MustRegisterWorkflow(def)

	backends := testkit.NewMemoryBackends()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	policy := gostage.FailurePolicyFunc(func(_ context.Context, info gostage.FailureContext) gostage.FailureDecision {
		if info.Attempt >= 1 {
			return gostage.FailureDecisionAck
		}
		return gostage.FailureDecisionAck
	})

	node, diag, err := gostage.Run(ctx, append(testkit.MemoryOptions(backends),
		gostage.WithFailurePolicy(policy),
		gostage.WithPool(gostage.PoolConfig{Name: "primary", Tags: []string{"primary"}, Slots: 1}),
	)...)
	if err != nil {
		t.Fatalf("run error: %v", err)
	}
	collector := testkit.StartDiagnosticsCollector(t, diag)
	t.Cleanup(collector.Close)
	t.Cleanup(func() { _ = node.Close() })

	teleBuf := testkit.StartTelemetryBuffer(ctx, t, node, 64)
	t.Cleanup(teleBuf.Close)
	healthBuf := testkit.StartHealthBuffer(ctx, t, node, 64)
	t.Cleanup(healthBuf.Close)

	runFail, err := node.Submit(ctx, gostage.WorkflowRef(workflowID))
	if err != nil {
		t.Fatalf("submit fail: %v", err)
	}

	waitCtx, cancelWait := context.WithTimeout(ctx, 5*time.Second)
	defer cancelWait()
	result, err := node.Wait(waitCtx, runFail)
	if err != nil {
		t.Fatalf("wait fail: %v", err)
	}
	if result.Success {
		t.Fatalf("expected failure result, got %+v", result)
	}

	degradeEvt := healthBuf.Next(t, "degraded", 5*time.Second)
	if degradeEvt.Detail == "" {
		t.Fatalf("expected degrade detail message")
	}

	runSuccess, err := node.Submit(ctx, gostage.WorkflowRef(workflowID))
	if err != nil {
		t.Fatalf("submit success: %v", err)
	}

	waitCtx2, cancelWait2 := context.WithTimeout(ctx, 5*time.Second)
	defer cancelWait2()
	successResult, err := node.Wait(waitCtx2, runSuccess)
	if err != nil {
		t.Fatalf("wait success: %v", err)
	}
	if !successResult.Success {
		t.Fatalf("expected success result, got %+v", successResult)
	}

	healthyEvt := healthBuf.Next(t, "healthy", 5*time.Second)
	if healthyEvt.Detail == "" {
		t.Fatalf("expected healthy detail message")
	}

	summaryFail := testkit.AwaitWorkflowSummary(t, node.State, backends.Observer, ctx, runFail)
	if summaryFail.State != state.WorkflowFailed {
		t.Fatalf("expected failed state, got %s", summaryFail.State)
	}
	summarySuccess := testkit.AwaitWorkflowSummary(t, node.State, backends.Observer, ctx, runSuccess)
	if summarySuccess.State != state.WorkflowCompleted {
		t.Fatalf("expected completed state, got %s", summarySuccess.State)
	}

	failureSummary := teleBuf.Next(t, telemetry.EventWorkflowSummary, 5*time.Second)
	if success, _ := failureSummary.Metadata["success"].(bool); success {
		t.Fatalf("expected failure summary first, got %+v", failureSummary.Metadata)
	}
	successSummary := teleBuf.Next(t, telemetry.EventWorkflowSummary, 5*time.Second)
	if success, _ := successSummary.Metadata["success"].(bool); !success {
		t.Fatalf("expected success summary, got %+v", successSummary.Metadata)
	}
}
