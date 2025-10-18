package testkit

import (
	"context"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3/state"
)

// AwaitWorkflowSummary polls the state facade until the summary is available or times out.
func AwaitWorkflowSummary(t *testing.T, reader state.StateReader, fallback *state.CaptureObserver, ctx context.Context, id state.WorkflowID) state.WorkflowSummary {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		summary, err := reader.WorkflowSummary(ctx, id)
		if err == nil {
			return summary
		}
		if fallback != nil {
			snap := fallback.Snapshot()
			if _, exists := snap.Workflows[id]; exists {
				t.Logf("await summary: err=%v state=%v", err, snap.Workflows[id])
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("workflow summary: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// AwaitWorkflowSummaryWithReason waits until the summary carries the expected termination reason.
func AwaitWorkflowSummaryWithReason(t *testing.T, reader state.StateReader, ctx context.Context, id state.WorkflowID, reason state.TerminationReason) state.WorkflowSummary {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		summary, err := reader.WorkflowSummary(ctx, id)
		if err != nil {
			t.Fatalf("workflow summary: %v", err)
		}
		if summary.TerminationReason == reason {
			return summary
		}
		if time.Now().After(deadline) {
			t.Fatalf("workflow summary reason mismatch: got %s want %s summary=%+v", summary.TerminationReason, reason, summary)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
