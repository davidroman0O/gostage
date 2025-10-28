package testkit

import (
	"context"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3/state"
	statetest "github.com/davidroman0O/gostage/v3/state/testkit"
)

// AwaitWorkflowSummary polls the state facade until the summary is available or times out.
func AwaitWorkflowSummary(t *testing.T, reader state.StateReader, fallback *statetest.CaptureObserver, ctx context.Context, id state.WorkflowID) state.WorkflowSummary {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
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

// WaitForWorkflowInState polls ListWorkflows until the workflow appears in the
// requested state. It fails the test if the workflow does not surface within
// the deadline.
func WaitForWorkflowInState(t *testing.T, reader state.StateReader, id state.WorkflowID, target state.WorkflowState) state.WorkflowSummary {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var lastErr error
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		summaries, err := reader.ListWorkflows(ctx, state.StateFilter{States: []state.WorkflowState{target}})
		cancel()
		if err == nil {
			for _, summary := range summaries {
				if summary.ID == id {
					if summary.State != target {
						t.Fatalf("workflow %s listed with state %s, expected %s", id, summary.State, target)
					}
					return summary
				}
			}
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				t.Fatalf("workflow %s not found in ListWorkflows state=%s (last error: %v)", id, target, lastErr)
			}
			t.Fatalf("workflow %s not found in ListWorkflows state=%s", id, target)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
