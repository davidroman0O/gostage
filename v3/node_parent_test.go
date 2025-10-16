package gostage

import (
	"context"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/pools"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/davidroman0O/gostage/v3/workflow"
)

type stubQueue struct {
	stats   state.QueueStats
	pending int
}

func (s stubQueue) Enqueue(context.Context, workflow.Definition, state.Priority, map[string]any) (state.WorkflowID, error) {
	return "", nil
}

func (s stubQueue) Claim(context.Context, state.Selector, string) (*state.ClaimedWorkflow, error) {
	return nil, state.ErrNoPending
}

func (s stubQueue) Release(context.Context, state.WorkflowID) error { return nil }
func (s stubQueue) Ack(context.Context, state.WorkflowID, state.ResultSummary) error {
	return nil
}
func (s stubQueue) Cancel(context.Context, state.WorkflowID) error { return nil }
func (s stubQueue) Stats(context.Context) (state.QueueStats, error) {
	return s.stats, nil
}
func (s stubQueue) PendingCount(context.Context, state.Selector) (int, error) {
	return s.pending, nil
}
func (s stubQueue) Close() error { return nil }

func TestParentStatsReportsPoolMetadata(t *testing.T) {
	ctx := context.Background()
	health := node.NewHealthDispatcher()
	pool := pools.NewLocal("local", state.Selector{}, 2)
	bindings := []*poolBinding{{pool: pool}}
	dispatcher := newDispatcher(ctx, nil, nil, nil, nil, nil, nil, health, telemetry.NoopLogger{}, 0, 0, 0, nil, bindings)

	dispatcher.publishHealth("local", node.HealthUnavailable, "boom")
	// Ensure timestamp difference between events.
	time.Sleep(10 * time.Millisecond)
	dispatcher.publishHealth("local", node.HealthHealthy, "recovered")

	parent := &parentNode{
		dispatcher: dispatcher,
		queue:      stubQueue{stats: state.QueueStats{Pending: 3, Claimed: 1}, pending: 2},
		pools:      bindings,
	}

	snapshot, err := parent.stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if snapshot.QueueDepth != 3 {
		t.Fatalf("expected queue depth 3, got %d", snapshot.QueueDepth)
	}
	if len(snapshot.Pools) != 1 {
		t.Fatalf("expected 1 pool snapshot, got %d", len(snapshot.Pools))
	}
	poolSnap := snapshot.Pools[0]
	if !poolSnap.Healthy {
		t.Fatalf("expected pool to be healthy in snapshot")
	}
	if poolSnap.Status != node.HealthHealthy {
		t.Fatalf("expected status healthy, got %s", poolSnap.Status)
	}
	if poolSnap.LastHealthChange.IsZero() {
		t.Fatalf("expected last health change timestamp")
	}
	if poolSnap.LastError == nil || poolSnap.LastError.Error() != "boom" {
		t.Fatalf("expected last error to persist, got %v", poolSnap.LastError)
	}
	if poolSnap.LastErrorAt.IsZero() {
		t.Fatalf("expected last error timestamp to be recorded")
	}
	if poolSnap.Slots != 2 {
		t.Fatalf("expected slots to reflect pool capacity, got %d", poolSnap.Slots)
	}
	if poolSnap.Busy != 0 {
		t.Fatalf("expected no busy slots, got %d", poolSnap.Busy)
	}
	if poolSnap.Pending != 2 {
		t.Fatalf("expected pending count 2, got %d", poolSnap.Pending)
	}
}
