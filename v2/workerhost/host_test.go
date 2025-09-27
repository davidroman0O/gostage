package workerhost

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v2/broker"
	"github.com/davidroman0O/gostage/v2/registry"
	"github.com/davidroman0O/gostage/v2/runner"
	"github.com/davidroman0O/gostage/v2/runtime/local"
	"github.com/davidroman0O/gostage/v2/state"
	"github.com/davidroman0O/gostage/v2/state/memory"
	"github.com/davidroman0O/gostage/v2/types"
	"github.com/davidroman0O/gostage/v2/types/defaults"
)

type noopBroker struct{}

func (noopBroker) WorkflowRegistered(context.Context, state.WorkflowRecord) error         { return nil }
func (noopBroker) WorkflowStatus(context.Context, string, state.WorkflowState) error      { return nil }
func (noopBroker) StageRegistered(context.Context, string, state.StageRecord) error       { return nil }
func (noopBroker) StageStatus(context.Context, string, string, state.WorkflowState) error { return nil }
func (noopBroker) ActionRegistered(context.Context, string, string, state.ActionRecord) error {
	return nil
}
func (noopBroker) ActionStatus(context.Context, string, string, string, state.WorkflowState) error {
	return nil
}
func (noopBroker) ActionProgress(context.Context, string, string, string, int, string) error {
	return nil
}
func (noopBroker) ActionRemoved(context.Context, string, string, string, string) error   { return nil }
func (noopBroker) StageRemoved(context.Context, string, string, string) error            { return nil }
func (noopBroker) ExecutionSummary(context.Context, string, state.ExecutionReport) error { return nil }

func newTestHost(t *testing.T, slots int) (Host, registry.Registry) {
	t.Helper()

	reg := registry.NewSafeRegistry()
	prev := registry.Default()
	registry.SetDefault(reg)
	t.Cleanup(func() { registry.SetDefault(prev) })

	host, err := New(Config{
		InitialSlots:  slots,
		Factory:       local.Factory{},
		Registry:      reg,
		BrokerBuilder: func() (broker.Broker, error) { return noopBroker{}, nil },
		RunnerOptions: []runner.Option{},
	})
	if err != nil {
		t.Fatalf("New host: %v", err)
	}
	t.Cleanup(func() { _ = host.Stop(context.Background()) })
	return host, reg
}

func newIntegrationHost(t *testing.T, slots int) (Host, registry.Registry, *memory.Manager) {
	t.Helper()

	mgr := memory.New()
	reg := registry.NewSafeRegistry()
	prev := registry.Default()
	registry.SetDefault(reg)
	t.Cleanup(func() { registry.SetDefault(prev) })

	host, err := New(Config{
		InitialSlots:  slots,
		Factory:       local.Factory{},
		Registry:      reg,
		BrokerBuilder: func() (broker.Broker, error) { return broker.NewLocal(mgr), nil },
	})
	if err != nil {
		t.Fatalf("New integration host: %v", err)
	}
	t.Cleanup(func() { _ = host.Stop(context.Background()) })
	return host, reg, mgr
}

func createWorkflow(t *testing.T, reg registry.Registry, fn func(types.Context) error) types.Workflow {
	t.Helper()

	actionID := fmt.Sprintf("action-%d", time.Now().UnixNano())
	if err := reg.RegisterAction(actionID, func(ctx types.Context) error { return fn(ctx) }, registry.ActionMetadata{}); err != nil {
		t.Fatalf("register action: %v", err)
	}

	stageID := fmt.Sprintf("stage-%d", time.Now().UnixNano())
	stage := defaults.NewStage(stageID, "Stage", "")
	stage.AddActions(defaults.NewAction(actionID))

	workflowID := fmt.Sprintf("workflow-%d", time.Now().UnixNano())
	wf := defaults.NewWorkflow(workflowID, "Workflow", "")
	wf.AddStage(stage)
	return wf
}

func drainSlotAdded(events <-chan Event, expected int, timeout time.Duration) []Event {
	drained := make([]Event, 0)
	deadline := time.After(timeout)
	for len(drained) < expected {
		select {
		case evt := <-events:
			if evt.Type == EventSlotAdded {
				drained = append(drained, evt)
			}
		case <-deadline:
			return drained
		}
	}
	return drained
}

func collectEvents(events <-chan Event, count int, timeout time.Duration) []Event {
	collected := make([]Event, 0, count)
	deadline := time.After(timeout)
	for len(collected) < count {
		select {
		case evt := <-events:
			collected = append(collected, evt)
		case <-deadline:
			return collected
		}
	}
	return collected
}

func expectNoEvent(t *testing.T, events <-chan Event, wait time.Duration) {
	t.Helper()
	select {
	case evt := <-events:
		t.Fatalf("unexpected event received: %+v", evt)
	case <-time.After(wait):
	}
}

func TestWorkerHostBasicFlow(t *testing.T) {
	host, reg := newTestHost(t, 1)
	wf := createWorkflow(t, reg, func(types.Context) error { return nil })

	// drain initial slot-added event
	drainSlotAdded(host.Events(), 1, time.Second)

	job := Job{Workflow: wf, LeaseID: "lease-1"}
	if err := host.Submit(job); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	events := collectEvents(host.Events(), 3, time.Second)
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}

	if events[0].Type != EventAssigned || events[1].Type != EventStarted || events[2].Type != EventCompleted {
		t.Fatalf("unexpected event order: %+v", events)
	}

	stats := host.Stats()
	if stats.TotalSlots != 1 || stats.IdleSlots != 1 || stats.BusySlots != 0 || stats.Inflight != 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestWorkerHostFailure(t *testing.T) {
	host, reg := newTestHost(t, 1)
	wf := createWorkflow(t, reg, func(types.Context) error { return errors.New("boom") })

	drainSlotAdded(host.Events(), 1, time.Second)

	if err := host.Submit(Job{Workflow: wf, LeaseID: "lease-fail"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	events := collectEvents(host.Events(), 3, time.Second)
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	if events[2].Type != EventFailed || events[2].Err == nil {
		t.Fatalf("expected failure event, got %+v", events[2])
	}
}

func TestWorkerHostCancelBeforeAssignment(t *testing.T) {
	host, reg := newTestHost(t, 1)
	wf := createWorkflow(t, reg, func(types.Context) error { return nil })

	drainSlotAdded(host.Events(), 1, time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := host.Submit(Job{Workflow: wf, LeaseID: "lease-cancel", Context: ctx}); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	evt := <-host.Events()
	if evt.Type != EventCancelled {
		t.Fatalf("expected cancel event, got %+v", evt)
	}
}

func TestWorkerHostScaleUpDown(t *testing.T) {
	host, _ := newTestHost(t, 1)

	// drain initial event
	drainSlotAdded(host.Events(), 1, time.Second)

	if err := host.Scale(2); err != nil {
		t.Fatalf("Scale up: %v", err)
	}

	added := collectEvents(host.Events(), 2, time.Second)
	if len(added) != 2 {
		t.Fatalf("expected two slot added events, got %d", len(added))
	}

	stats := host.Stats()
	if stats.TotalSlots != 3 {
		t.Fatalf("expected 3 total slots, got %d", stats.TotalSlots)
	}

	if err := host.Scale(-2); err != nil {
		t.Fatalf("Scale down: %v", err)
	}

	removed := collectEvents(host.Events(), 2, time.Second)
	countRemoved := 0
	for _, evt := range removed {
		if evt.Type == EventSlotRemoved {
			countRemoved++
		}
	}
	if countRemoved != 2 {
		t.Fatalf("expected 2 slot removed events, got %d", countRemoved)
	}

	stats = host.Stats()
	if stats.TotalSlots != 1 {
		t.Fatalf("expected 1 total slot, got %d", stats.TotalSlots)
	}
}

func TestWorkerHostStop(t *testing.T) {
	host, reg := newTestHost(t, 1)
	wf := createWorkflow(t, reg, func(types.Context) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	drainSlotAdded(host.Events(), 1, time.Second)

	if err := host.Submit(Job{Workflow: wf, LeaseID: "lease-stop"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = host.Stop(context.Background())
	}()

	// events channel should eventually close
	for evt := range host.Events() {
		if evt.Type == EventCompleted {
			break
		}
	}
}

func TestWorkerHostAcquireAndStart(t *testing.T) {
	host, reg := newTestHost(t, 1)
	wf := createWorkflow(t, reg, func(types.Context) error { return nil })

	drainSlotAdded(host.Events(), 1, time.Second)

	lease, err := host.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	stats := host.Stats()
	if stats.TotalSlots != 1 || stats.IdleSlots != 0 || stats.BusySlots != 1 || stats.Inflight != 0 {
		t.Fatalf("unexpected stats after acquire: %+v", stats)
	}

	job := Job{ID: "lease-job", Workflow: wf}
	if err := lease.Start(job); err != nil {
		t.Fatalf("Start: %v", err)
	}

	events := collectEvents(host.Events(), 3, time.Second)
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	if events[0].Type != EventAssigned || events[0].SlotID != lease.ID() {
		t.Fatalf("unexpected assigned event: %+v", events[0])
	}
	if events[2].Type != EventCompleted {
		t.Fatalf("expected completion event, got %+v", events[2])
	}

	stats = host.Stats()
	if stats.TotalSlots != 1 || stats.IdleSlots != 1 || stats.BusySlots != 0 || stats.Inflight != 0 {
		t.Fatalf("unexpected stats after completion: %+v", stats)
	}

	if err := lease.Release(); err == nil {
		t.Fatalf("expected release after start to error")
	}
}

func TestWorkerHostAcquireBlocksUntilJobCompletes(t *testing.T) {
	host, reg := newTestHost(t, 1)
	block := make(chan struct{})
	wf := createWorkflow(t, reg, func(types.Context) error {
		<-block
		return nil
	})

	drainSlotAdded(host.Events(), 1, time.Second)

	lease1, err := host.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := lease1.Start(Job{Workflow: wf}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	leaseCh := make(chan Lease, 1)
	errCh := make(chan error, 1)
	go func() {
		lease, err := host.Acquire(context.Background())
		if err != nil {
			errCh <- err
			return
		}
		leaseCh <- lease
	}()

	select {
	case <-leaseCh:
		t.Fatalf("expected second acquire to block while job running")
	case err := <-errCh:
		t.Fatalf("unexpected acquire error: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(block)
	collectEvents(host.Events(), 3, time.Second)

	var lease2 Lease
	select {
	case lease2 = <-leaseCh:
	case err := <-errCh:
		t.Fatalf("acquire returned error: %v", err)
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for second lease")
	}

	if err := lease2.Release(); err != nil {
		t.Fatalf("release second lease: %v", err)
	}

	if err := lease1.Release(); err == nil {
		t.Fatalf("expected release after start to error")
	}
}

func TestWorkerHostAcquireCancelledOnStop(t *testing.T) {
	host, _ := newTestHost(t, 1)
	drainSlotAdded(host.Events(), 1, time.Second)

	lease, err := host.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire initial: %v", err)
	}

	leaseCh := make(chan Lease, 1)
	errCh := make(chan error, 1)
	go func() {
		lease, err := host.Acquire(context.Background())
		if err != nil {
			errCh <- err
			return
		}
		leaseCh <- lease
	}()

	select {
	case <-leaseCh:
		t.Fatalf("expected second acquire to block before stop")
	case err := <-errCh:
		t.Fatalf("unexpected acquire error: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	if err := host.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrHostStopped) {
			t.Fatalf("expected ErrHostStopped, got %v", err)
		}
	case <-leaseCh:
		t.Fatalf("expected acquire to error, not succeed")
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for acquire failure")
	}

	if err := lease.Release(); !errors.Is(err, ErrHostStopped) {
		t.Fatalf("expected release to return ErrHostStopped, got %v", err)
	}
}

func TestWorkerHostSubmitWaitsForReservedLease(t *testing.T) {
	host, reg := newTestHost(t, 1)
	wf := createWorkflow(t, reg, func(types.Context) error { return nil })

	drainSlotAdded(host.Events(), 1, time.Second)

	lease, err := host.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	if err := host.Submit(Job{Workflow: wf, LeaseID: "queued"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	expectNoEvent(t, host.Events(), 20*time.Millisecond)

	if err := lease.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}

	events := collectEvents(host.Events(), 3, time.Second)
	if len(events) != 3 {
		t.Fatalf("expected 3 events after release, got %d", len(events))
	}
	if events[0].Type != EventAssigned || events[0].LeaseID != "queued" {
		t.Fatalf("unexpected first event: %+v", events[0])
	}
}

func TestCoordinatorLoopWithWorkerHost(t *testing.T) {
	host, reg, manager := newIntegrationHost(t, 1)

	eventCtx, cancelEvents := context.WithCancel(context.Background())
	events := make(chan Event, 32)
	go func() {
		defer close(events)
		for {
			select {
			case <-eventCtx.Done():
				return
			case evt, ok := <-host.Events():
				if !ok {
					return
				}
				if evt.Type == EventCompleted || evt.Type == EventFailed {
					success := evt.Type == EventCompleted
					msg := ""
					if evt.Err != nil {
						msg = evt.Err.Error()
					} else if evt.Result.Error != nil {
						msg = evt.Result.Error.Error()
					}
					_ = manager.CompleteWorkflow(evt.JobID, &state.WorkflowResult{
						Success: success,
						Error:   msg,
						Output:  evt.Result.FinalStore,
						EndedAt: time.Now(),
					})
				}
				select {
				case events <- evt:
				default:
				}
			}
		}
	}()

	errCh := make(chan error, 1)
	coord := newMockCoordinator(host, manager, errCh)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go coord.Run(ctx)

	wf1 := createWorkflow(t, reg, func(ctx types.Context) error {
		if store := ctx.Store(); store != nil {
			_ = store.Put("result", ctx.Workflow().ID())
		}
		return nil
	})
	wf2 := createWorkflow(t, reg, func(ctx types.Context) error {
		time.Sleep(20 * time.Millisecond)
		if store := ctx.Store(); store != nil {
			_ = store.Put("result", ctx.Workflow().ID())
		}
		return nil
	})

	coord.Enqueue(state.SubWorkflowDef{
		ID:        wf1.ID(),
		Name:      wf1.Name(),
		Type:      "integration-test",
		Tags:      []string{"integration"},
		Priority:  state.PriorityDefault,
		CreatedAt: time.Now(),
	}, wf1)
	coord.Enqueue(state.SubWorkflowDef{
		ID:        wf2.ID(),
		Name:      wf2.Name(),
		Type:      "integration-test",
		Tags:      []string{"integration"},
		Priority:  state.PriorityDefault,
		CreatedAt: time.Now(),
	}, wf2)

	for _, wf := range []types.Workflow{wf1, wf2} {
		res, err := manager.WaitForCompletion(wf.ID(), time.Second)
		if err != nil {
			t.Fatalf("WaitForCompletion %s: %v", wf.ID(), err)
		}
		if !res.Success {
			t.Fatalf("workflow %s failed: %+v", wf.ID(), res)
		}
	}

	completed := 0
	deadline := time.After(time.Second)
	for completed < 2 {
		select {
		case evt, ok := <-events:
			if !ok {
				t.Fatalf("events channel closed early")
			}
			if evt.Type == EventCompleted {
				completed++
			}
		case <-deadline:
			t.Fatalf("timed out waiting for completion events")
		}
	}

	for _, wf := range []types.Workflow{wf1, wf2} {
		summary := waitForSummary(t, manager, wf.ID())
		if summary == nil {
			t.Fatalf("no summary for %s", wf.ID())
		}
		if !summary.Success {
			t.Fatalf("summary for %s indicates failure", wf.ID())
		}
		if value, ok := summary.FinalStore["result"]; !ok || value != wf.ID() {
			t.Fatalf("unexpected final store for %s: %+v", wf.ID(), summary.FinalStore)
		}
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("coordinator error: %v", err)
		}
	default:
	}

	cancel()
	cancelEvents()
	if err := host.Stop(context.Background()); err != nil && !errors.Is(err, ErrHostStopped) {
		t.Fatalf("host stop: %v", err)
	}
}

func TestWorkerHostAcquireRelease(t *testing.T) {
	host, _ := newTestHost(t, 1)
	drainSlotAdded(host.Events(), 1, time.Second)

	lease, err := host.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	if err := lease.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}

	stats := host.Stats()
	if stats.IdleSlots != 1 || stats.BusySlots != 0 || stats.Inflight != 0 {
		t.Fatalf("unexpected stats after release: %+v", stats)
	}

	if err := lease.Release(); err == nil {
		t.Fatalf("expected second release to fail")
	}
}

func TestWorkerHostAcquireCancelled(t *testing.T) {
	host, _ := newTestHost(t, 1)
	drainSlotAdded(host.Events(), 1, time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := host.Acquire(ctx); err == nil {
		t.Fatalf("expected acquire cancellation error")
	}

	stats := host.Stats()
	if stats.IdleSlots != 1 || stats.BusySlots != 0 || stats.Inflight != 0 {
		t.Fatalf("unexpected stats after cancelled acquire: %+v", stats)
	}
}

func waitForSummary(t *testing.T, manager *memory.Manager, workflowID string) *state.ExecutionReport {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		summary, err := manager.GetExecutionSummary(context.Background(), workflowID)
		if err == nil {
			return summary
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for summary %s: %v", workflowID, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type mockCoordinator struct {
	host      Host
	manager   *memory.Manager
	workflows map[string]types.Workflow
	mu        sync.Mutex
	errCh     chan<- error
}

func newMockCoordinator(host Host, manager *memory.Manager, errCh chan<- error) *mockCoordinator {
	return &mockCoordinator{
		host:      host,
		manager:   manager,
		workflows: make(map[string]types.Workflow),
		errCh:     errCh,
	}
}

func (c *mockCoordinator) Enqueue(def state.SubWorkflowDef, wf types.Workflow) {
	c.mu.Lock()
	c.workflows[def.ID] = wf
	c.mu.Unlock()
	if def.ID == "" {
		def.ID = wf.ID()
	}
	def.Priority = state.PriorityDefault
	if def.CreatedAt.IsZero() {
		def.CreatedAt = time.Now()
	}
	if def.ScheduledAt.IsZero() {
		def.ScheduledAt = def.CreatedAt
	}
	if id := c.manager.StoreWithPriority(def, def.Priority); id == "" {
		c.report(fmt.Errorf("failed to store workflow %s", def.ID))
	}
}

func (c *mockCoordinator) Run(ctx context.Context) {
	backoff := time.NewTicker(5 * time.Millisecond)
	defer backoff.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		lease, err := c.host.Acquire(ctx)
		if err != nil {
			if errors.Is(err, ErrHostStopped) || errors.Is(err, context.Canceled) {
				return
			}
			continue
		}
		queued, err := c.manager.ClaimWorkflow(lease.ID(), "integration-worker", state.WorkflowFilter{})
		if err != nil {
			if err := lease.Release(); err != nil && !errors.Is(err, ErrHostStopped) {
				c.report(fmt.Errorf("lease release: %w", err))
			}
			select {
			case <-ctx.Done():
				return
			case <-backoff.C:
			}
			continue
		}
		wf := c.takeWorkflow(queued.Status.ID)
		if wf == nil {
			if err := lease.Release(); err != nil && !errors.Is(err, ErrHostStopped) {
				c.report(fmt.Errorf("lease release missing workflow: %w", err))
			}
			continue
		}
		job := Job{
			ID:       queued.Status.ID,
			LeaseID:  queued.Status.ID,
			Workflow: wf,
			Metadata: map[string]interface{}{"origin": "mock-coordinator"},
		}
		if err := lease.Start(job); err != nil {
			if !errors.Is(err, ErrHostStopped) {
				c.report(fmt.Errorf("lease start: %w", err))
			}
			return
		}
	}
}

func (c *mockCoordinator) takeWorkflow(id string) types.Workflow {
	c.mu.Lock()
	defer c.mu.Unlock()
	wf, ok := c.workflows[id]
	if ok {
		delete(c.workflows, id)
	}
	return wf
}

func (c *mockCoordinator) report(err error) {
	if err == nil || c.errCh == nil {
		return
	}
	select {
	case c.errCh <- err:
	default:
	}
}
