package gostage

import (
	"context"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3/pools"
	"github.com/davidroman0O/gostage/v3/state"
)

func TestRemoteCoordinatorOnLeaseAckReleasesJob(t *testing.T) {
	ctx := context.Background()
	diag := &diagCollector{}

	binding := &poolBinding{pool: pools.NewLocal("remote", state.Selector{}, 1)}
	rc, _ := buildRemoteCoordinatorForTest(t, ctx, diag, []*poolBinding{binding})

	remotePool := rc.pools["remote"]
	remotePool.worker = &remoteWorker{busy: true}

	releaseCalled := false
	workflowID := "workflow-1"
	rc.jobs[workflowID] = &remoteJob{
		workflow: &state.ClaimedWorkflow{
			QueuedWorkflow: state.QueuedWorkflow{
				ID: state.WorkflowID(workflowID),
			},
			LeaseID:   "lease-1",
			ClaimedAt: time.Now(),
		},
		pool:    remotePool,
		release: func() { releaseCalled = true },
	}

	rc.dispatcher.inflight.Add(1)
	rc.dispatcher.wg.Add(1)

	summary := state.ResultSummary{Success: true}
	if err := rc.OnLeaseAck(ctx, nil, workflowID, "lease-1", summary); err != nil {
		t.Fatalf("OnLeaseAck returned error: %v", err)
	}

	if _, ok := rc.jobs[workflowID]; ok {
		t.Fatalf("expected job to be removed after ack")
	}
	if !releaseCalled {
		t.Fatalf("expected release callback to be invoked")
	}
	if remotePool.worker == nil || remotePool.worker.busy {
		t.Fatalf("expected worker to be marked idle after ack")
	}
}

func TestRemoteCoordinatorOnLeaseAckUnknownWorkflow(t *testing.T) {
	ctx := context.Background()
	diag := &diagCollector{}

	binding := &poolBinding{pool: pools.NewLocal("remote", state.Selector{}, 1)}
	rc, _ := buildRemoteCoordinatorForTest(t, ctx, diag, []*poolBinding{binding})

	err := rc.OnLeaseAck(ctx, nil, "missing", "lease", state.ResultSummary{})
	if err == nil {
		t.Fatalf("expected error for unknown workflow ack")
	}
}
