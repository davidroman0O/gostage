package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3"
	"github.com/davidroman0O/gostage/v3/e2e/testkit"
	rt "github.com/davidroman0O/gostage/v3/runtime"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/davidroman0O/gostage/v3/workflow"
)

func TestConcurrencyAndFairnessAcrossPools(t *testing.T) {
	testkit.ResetRegistry(t)

	gostage.MustRegisterAction("work.cpu", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			time.Sleep(50 * time.Millisecond)
			if broker := ctx.Broker(); broker != nil {
				_ = broker.Progress(50, "cpu")
			}
			return nil
		}
	})

	gostage.MustRegisterAction("work.gpu", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			time.Sleep(50 * time.Millisecond)
			if broker := ctx.Broker(); broker != nil {
				_ = broker.Progress(50, "gpu")
			}
			return nil
		}
	})

	cpuWorkflowID, _ := gostage.MustRegisterWorkflow(workflow.Definition{
		Name: "CPU Workflow",
		Tags: []string{"cpu"},
		Stages: []workflow.Stage{{
			Name:    "stage",
			Actions: []workflow.Action{{Ref: "work.cpu"}},
		}},
	})

	gpuWorkflowID, _ := gostage.MustRegisterWorkflow(workflow.Definition{
		Name: "GPU Workflow",
		Tags: []string{"gpu"},
		Stages: []workflow.Stage{{
			Name:    "stage",
			Actions: []workflow.Action{{Ref: "work.gpu"}},
		}},
	})

	backends := testkit.NewMemoryBackends()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, diag, err := gostage.Run(ctx,
		append(testkit.MemoryOptions(backends),
			gostage.WithPool(gostage.PoolConfig{Name: "cpu", Tags: []string{"cpu"}, Slots: 1}),
			gostage.WithPool(gostage.PoolConfig{Name: "gpu", Tags: []string{"gpu"}, Slots: 1}),
			gostage.WithDispatcherConfig(gostage.DispatcherConfig{MaxInFlight: 2, ClaimInterval: 10 * time.Millisecond}),
		)...,
	)
	if err != nil {
		t.Fatalf("run error: %v", err)
	}
	collector := testkit.StartDiagnosticsCollector(t, diag)
	t.Cleanup(collector.Close)
	t.Cleanup(func() { _ = node.Close() })

	teleBuf := testkit.StartTelemetryBuffer(ctx, t, node, 256)
	t.Cleanup(teleBuf.Close)
	healthBuf := testkit.StartHealthBuffer(ctx, t, node, 64)
	t.Cleanup(healthBuf.Close)

	type run struct {
		id       state.WorkflowID
		expected string
	}
	runs := make([]run, 0, 6)

	for i := 0; i < 3; i++ {
		id, err := node.Submit(ctx, gostage.WorkflowRef(cpuWorkflowID), gostage.WithTags("cpu"))
		if err != nil {
			t.Fatalf("submit cpu: %v", err)
		}
		runs = append(runs, run{id: id, expected: "cpu"})
	}
	for i := 0; i < 3; i++ {
		id, err := node.Submit(ctx, gostage.WorkflowRef(gpuWorkflowID), gostage.WithTags("gpu"))
		if err != nil {
			t.Fatalf("submit gpu: %v", err)
		}
		runs = append(runs, run{id: id, expected: "gpu"})
	}

	// Ensure both pools reach in-flight work.
	deadline := time.Now().Add(2 * time.Second)
	for {
		stats, err := node.Stats()
		if err != nil {
			t.Fatalf("stats during inflight wait: %v", err)
		}
		if stats.InFlight >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dispatcher never reached concurrent execution: %+v", stats)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Validate claim routing via telemetry metadata.
	claimsByPool := map[string]int{"cpu": 0, "gpu": 0}
	expectedCount := len(runs)
	observed := make(map[state.WorkflowID]string)
	for i := 0; i < expectedCount; i++ {
		evt := teleBuf.Next(t, telemetry.EventWorkflowClaimed, 3*time.Second)
		pool, _ := evt.Metadata["pool"].(string)
		if pool == "" {
			t.Fatalf("claim event missing pool metadata: %+v", evt.Metadata)
		}
		observed[state.WorkflowID(evt.WorkflowID)] = pool
		claimsByPool[pool]++
	}

	for _, r := range runs {
		pool, ok := observed[r.id]
		if !ok {
			t.Fatalf("missing claim telemetry for %s", r.id)
		}
		if pool != r.expected {
			t.Fatalf("workflow %s expected pool %s, claimed by %s", r.id, r.expected, pool)
		}
	}
	if claimsByPool["cpu"] == 0 || claimsByPool["gpu"] == 0 {
		t.Fatalf("expected claims on both pools, got %+v", claimsByPool)
	}

	// Wait for all workflows to complete.
	for _, r := range runs {
		waitCtx, cancelWait := context.WithTimeout(ctx, 10*time.Second)
		result, err := node.Wait(waitCtx, r.id)
		cancelWait()
		if err != nil {
			t.Fatalf("wait %s: %v", r.id, err)
		}
		if !result.Success {
			t.Fatalf("workflow %s failed: %+v", r.id, result)
		}
	}

	// Drain health events until both pools report healthy.
	poolsHealthy := map[string]bool{"cpu": false, "gpu": false}
	deadline = time.Now().Add(3 * time.Second)
	for !(poolsHealthy["cpu"] && poolsHealthy["gpu"]) {
		evt := healthBuf.Next(t, "healthy", 3*time.Second)
		poolsHealthy[evt.Pool] = true
		if time.Now().After(deadline) {
			break
		}
	}
	if !poolsHealthy["cpu"] || !poolsHealthy["gpu"] {
		t.Fatalf("expected healthy events for both pools, got %+v", poolsHealthy)
	}

	stats, err := node.Stats()
	if err != nil {
		t.Fatalf("stats final: %v", err)
	}
	if stats.Completed != len(runs) || stats.InFlight != 0 || stats.QueueDepth != 0 {
		t.Fatalf("unexpected final stats %+v", stats)
	}
	if len(stats.Pools) != 2 {
		t.Fatalf("expected 2 pool snapshots, got %d", len(stats.Pools))
	}
	for _, pool := range stats.Pools {
		if pool.Pending != 0 {
			t.Fatalf("pool %s has pending work: %+v", pool.Name, pool)
		}
		if !pool.Healthy {
			t.Fatalf("pool %s reported unhealthy at completion: %+v", pool.Name, pool)
		}
	}

	// Ensure state facade reports the expected tag distribution.
	cpuList, err := node.State.ListWorkflows(ctx, state.StateFilter{Tags: []string{"cpu"}})
	if err != nil {
		t.Fatalf("list cpu workflows: %v", err)
	}
	if len(cpuList) != 3 {
		t.Fatalf("expected 3 cpu workflows, got %d", len(cpuList))
	}
	gpuList, err := node.State.ListWorkflows(ctx, state.StateFilter{Tags: []string{"gpu"}})
	if err != nil {
		t.Fatalf("list gpu workflows: %v", err)
	}
	if len(gpuList) != 3 {
		t.Fatalf("expected 3 gpu workflows, got %d", len(gpuList))
	}
}
