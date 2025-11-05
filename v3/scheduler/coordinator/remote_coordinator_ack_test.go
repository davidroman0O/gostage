package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3/bootstrap"
	"github.com/davidroman0O/gostage/v3/pools"
	"github.com/davidroman0O/gostage/v3/state"
)

func TestRemoteCoordinatorOnLeaseAckReleasesJob(t *testing.T) {
	ctx := context.Background()
	diag := &diagCollector{}

	binding := &Binding{
		Pool: pools.NewLocal("remote", state.Selector{}, 1),
		Remote: &RemoteBinding{
			PoolCfg: bootstrap.PoolConfig{Name: "remote", Slots: 1},
		},
	}
	rc, _ := buildRemoteCoordinatorForTest(t, ctx, diag, []*Binding{binding})

	remotePool := rc.PoolsForTest()["remote"]
	remotePool.Worker = NewRemoteWorkerForTest(true, nil)

	releaseCalled := false
	workflowID := "workflow-1"
	rc.JobsForTest()[workflowID] = &RemoteJob{
		Workflow: &state.ClaimedWorkflow{
			QueuedWorkflow: state.QueuedWorkflow{
				ID: state.WorkflowID(workflowID),
			},
			LeaseID:   "lease-1",
			ClaimedAt: time.Now(),
		},
		Pool:    remotePool,
		Release: func() { releaseCalled = true },
	}

	rc.DispatcherForTest().BeginRemoteDispatch(state.WorkflowID(workflowID), func() {})

	summary := state.ResultSummary{Success: true}
	if err := rc.OnLeaseAck(ctx, nil, workflowID, "lease-1", summary); err != nil {
		t.Fatalf("OnLeaseAck returned error: %v", err)
	}

	if _, ok := rc.JobsForTest()[workflowID]; ok {
		t.Fatalf("expected job to be removed after ack")
	}
	if !releaseCalled {
		t.Fatalf("expected release callback to be invoked")
	}
	if remotePool.Worker == nil || remotePool.Worker.BusyForTest() {
		t.Fatalf("expected worker to be marked idle after ack")
	}
	if rc.DispatcherForTest().Inflight() != 0 {
		t.Fatalf("expected dispatcher inflight to be zero")
	}
}

func TestRemoteCoordinatorOnLeaseAckUnknownWorkflow(t *testing.T) {
	ctx := context.Background()
	diag := &diagCollector{}

	binding := &Binding{
		Pool: pools.NewLocal("remote", state.Selector{}, 1),
		Remote: &RemoteBinding{
			PoolCfg: bootstrap.PoolConfig{Name: "remote", Slots: 1},
		},
	}
	rc, _ := buildRemoteCoordinatorForTest(t, ctx, diag, []*Binding{binding})

	err := rc.OnLeaseAck(ctx, nil, "missing", "lease", state.ResultSummary{})
	if err == nil {
		t.Fatalf("expected error for unknown workflow ack")
	}
}
