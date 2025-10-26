package gostage

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3/broker"
	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/pools"
	"github.com/davidroman0O/gostage/v3/registry"
	"github.com/davidroman0O/gostage/v3/runner"
	rt "github.com/davidroman0O/gostage/v3/runtime"
	"github.com/davidroman0O/gostage/v3/runtime/local"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/davidroman0O/gostage/v3/workflow"
)

func TestDispatcherMaxInFlight(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	registry.SetDefault(registry.NewSafeRegistry())

	started := make(chan struct{}, 4)
	release := make(chan struct{})

	MustRegisterAction("test.block", func() ActionFunc {
		return func(actionCtx rt.Context) error {
			started <- struct{}{}
			select {
			case <-release:
			case <-actionCtx.Done():
				return actionCtx.Err()
			}
			return nil
		}
	})

	def := workflow.Definition{
		ID:   "wf-block",
		Name: "blocker",
		Stages: []workflow.Stage{
			{
				Name:    "Stage",
				Actions: []workflow.Action{{Ref: "test.block"}},
			},
		},
	}
	if normalized, _, err := workflow.EnsureIDs(def); err != nil {
		t.Fatalf("ensure ids: %v", err)
	} else {
		def = normalized
	}

	queue := state.NewMemoryQueue()
	if _, err := queue.Enqueue(ctx, def, state.PriorityDefault, nil); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := queue.Enqueue(ctx, def, state.PriorityDefault, nil); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	store := state.NewMemoryStore()
	manager, err := state.NewStoreManager(store)
	if err != nil {
		t.Fatalf("store manager: %v", err)
	}

	br := broker.NewLocal(manager)
	run := runner.New(local.Factory{}, registry.Default(), br)

	pool := pools.NewLocal("local", state.Selector{}, 2)
	bindings := []*poolBinding{{pool: pool}}

	dispatcher := newDispatcher(ctx, queue, store, manager, run, nil, nil, nil, telemetry.NoopLogger{}, 5*time.Millisecond, 0, 1, nil, bindings)
	dispatcher.start()
	defer dispatcher.stop()

	select {
	case <-started:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected first workflow to start")
	}

	select {
	case <-started:
		t.Fatalf("second workflow started despite maxInFlight=1")
	case <-time.After(150 * time.Millisecond):
	}

	release <- struct{}{}

	select {
	case <-started:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected second workflow to start after release")
	}
	release <- struct{}{}
}

func TestDispatcherPublishesHealthOnClaimError(t *testing.T) {
	ctx := context.Background()
	queue := &errorQueue{err: errors.New("boom")}
	health := node.NewHealthDispatcher()

	events := make(chan node.HealthEvent, 1)
	_ = health.Subscribe(func(evt node.HealthEvent) {
		events <- evt
	})

	dispatcher := newDispatcher(ctx, queue, nil, nil, nil, nil, nil, health, telemetry.NoopLogger{}, 0, 0, 0, nil, []*poolBinding{{pool: pools.NewLocal("local", state.Selector{}, 1)}})
	dispatcher.pollOnce()

	select {
	case evt := <-events:
		if evt.Status != node.HealthUnavailable {
			t.Fatalf("expected unavailable status, got %s", evt.Status)
		}
	default:
		t.Fatalf("expected health event to be published")
	}
}

func TestDispatcherHealthTracksLastError(t *testing.T) {
	ctx := context.Background()
	health := node.NewHealthDispatcher()
	pool := pools.NewLocal("local", state.Selector{}, 1)
	bindings := []*poolBinding{{pool: pool}}
	dispatcher := newDispatcher(ctx, nil, nil, nil, nil, nil, nil, health, telemetry.NoopLogger{}, 0, 0, 0, nil, bindings)

	events := make(chan node.HealthEvent, 2)
	_ = health.Subscribe(func(evt node.HealthEvent) {
		events <- evt
	})

	dispatcher.publishHealth("local", node.HealthUnavailable, "boom")
	first := <-events
	if first.Status != node.HealthUnavailable {
		t.Fatalf("expected unavailable status, got %s", first.Status)
	}

	status, detail, lastChange, lastErrDetail, lastErrAt := dispatcher.healthInfo("local")
	if status != node.HealthUnavailable {
		t.Fatalf("expected status unavailable, got %s", status)
	}
	if detail != "boom" {
		t.Fatalf("expected detail boom, got %s", detail)
	}
	if lastErrDetail != "boom" {
		t.Fatalf("expected last error detail to persist, got %s", lastErrDetail)
	}
	if lastErrAt.IsZero() {
		t.Fatalf("expected last error timestamp to be recorded")
	}
	if lastChange.IsZero() {
		t.Fatalf("expected last change timestamp")
	}

	dispatcher.publishHealth("local", node.HealthHealthy, "recovered")
	second := <-events
	if second.Status != node.HealthHealthy {
		t.Fatalf("expected healthy status, got %s", second.Status)
	}

	status, detail, lastChange2, lastErrDetail2, lastErrAt2 := dispatcher.healthInfo("local")
	if status != node.HealthHealthy {
		t.Fatalf("expected status healthy after recovery, got %s", status)
	}
	if detail != "recovered" {
		t.Fatalf("expected recovery detail, got %s", detail)
	}
	if lastErrDetail2 != "boom" {
		t.Fatalf("expected last error detail to persist after recovery, got %s", lastErrDetail2)
	}
	if !lastErrAt2.Equal(lastErrAt) {
		t.Fatalf("expected last error timestamp to remain stable, got %v vs %v", lastErrAt2, lastErrAt)
	}
	if !lastChange2.After(lastChange) {
		t.Fatalf("expected last health change to advance after recovery")
	}
}

func TestDispatcherStoresSummaryBeforeAck(t *testing.T) {
	ctx := context.Background()
	registry.SetDefault(registry.NewSafeRegistry())

	MustRegisterAction("test.success", func() ActionFunc {
		return func(rt.Context) error {
			return nil
		}
	})

	def := workflow.Definition{
		Name: "order-check",
		Stages: []workflow.Stage{
			{
				Name:    "stage",
				Actions: []workflow.Action{{Ref: "test.success"}},
			},
		},
	}
	normalized, _, err := workflow.EnsureIDs(def)
	if err != nil {
		t.Fatalf("ensure ids: %v", err)
	}

	baseQueue := state.NewMemoryQueue()
	spyStore := newSpyingStore(state.NewMemoryStore())
	manager, err := state.NewStoreManager(spyStore)
	if err != nil {
		t.Fatalf("store manager: %v", err)
	}

	queue := newSpyQueue(baseQueue, spyStore)
	br := broker.NewLocal(manager)
	run := runner.New(local.Factory{}, registry.Default(), br)

	pool := pools.NewLocal("local", state.Selector{}, 1)
	dispatcher := newDispatcher(ctx, queue, nil, manager, run, nil, nil, nil, telemetry.NoopLogger{}, 0, 0, 0, nil, []*poolBinding{{pool: pool}})

	if _, err := queue.Enqueue(ctx, normalized.Clone(), state.PriorityDefault, nil); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	release, ok := pool.TryAcquire(ctx)
	if !ok {
		t.Fatalf("expected pool acquisition")
	}
	claimed, err := queue.Claim(ctx, pool.Selector(), pool.Name())
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	dispatcher.inflight.Add(1)
	dispatcher.wg.Add(1)
	dispatcher.execute(pool, release, claimed)

	if dispatcher.inflight.Load() != 0 {
		t.Fatalf("expected inflight to be zero, got %d", dispatcher.inflight.Load())
	}
	if queue.ackCount.Load() != 1 {
		t.Fatalf("expected single ack call, got %d", queue.ackCount.Load())
	}
	if queue.ackBeforeStore.Load() {
		t.Fatalf("queue ack invoked before summary persisted")
	}
	if !spyStore.summaryStored.Load() {
		t.Fatalf("store summary was not persisted")
	}
}

func TestDispatcherSuppressesWorkflowTelemetry(t *testing.T) {
	ctx := context.Background()
	registry.SetDefault(registry.NewSafeRegistry())

	queue := state.NewMemoryQueue()
	store := state.NewMemoryStore()
	manager, err := state.NewStoreManager(store)
	if err != nil {
		t.Fatalf("store manager: %v", err)
	}

	telemetryDisp := node.NewTelemetryDispatcher(ctx, nil, node.TelemetryDispatcherConfig{})
	defer telemetryDisp.Close()
	health := node.NewHealthDispatcher()

	managerWithTelemetry := wrapWithTelemetry(manager, telemetryDisp)

	sink := telemetry.NewChannelSink(8)
	_ = telemetryDisp.Register(sink)

	br := broker.NewLocal(managerWithTelemetry)
	run := runner.New(local.Factory{}, registry.Default(), br, runner.WithDefaultLogger(telemetry.NoopLogger{}))

	pool := pools.NewLocal("remote", state.Selector{}, 1)
	dispatcher := newDispatcher(ctx, queue, store, managerWithTelemetry, run, telemetryDisp, nil, health, telemetry.NoopLogger{}, 0, 0, 0, nil, []*poolBinding{{pool: pool}})

	wf := state.WorkflowRecord{
		ID:   state.WorkflowID("wf-remote"),
		Name: "remote",
	}
	if err := dispatcher.manager.WorkflowRegistered(ctx, wf); err != nil {
		t.Fatalf("workflow registered: %v", err)
	}
	<-sink.C() // drain registration event

	dispatcher.suppressWorkflowTelemetry(wf.ID,
		telemetry.EventWorkflowStarted,
		telemetry.EventWorkflowCompleted,
		telemetry.EventWorkflowFailed,
		telemetry.EventWorkflowCancelled,
		telemetry.EventWorkflowSummary,
	)

	childEvt := telemetry.Event{
		Kind:       telemetry.EventWorkflowStarted,
		WorkflowID: string(wf.ID),
		Timestamp:  time.Now(),
	}
	if err := dispatcher.telemetry.Dispatch(childEvt); err != nil {
		t.Fatalf("dispatch child telemetry: %v", err)
	}
	startEvt := <-sink.C()
	if startEvt.Kind != telemetry.EventWorkflowStarted {
		t.Fatalf("expected workflow.started from child, got %s", startEvt.Kind)
	}

	if err := dispatcher.manager.WorkflowStatus(ctx, string(wf.ID), state.WorkflowRunning); err != nil {
		t.Fatalf("workflow status running: %v", err)
	}
	select {
	case evt := <-sink.C():
		t.Fatalf("unexpected telemetry event during suppression: %s", evt.Kind)
	case <-time.After(50 * time.Millisecond):
	}

	report := state.ExecutionReport{
		WorkflowID: string(wf.ID),
		Status:     state.WorkflowCompleted,
		Success:    true,
	}
	if err := dispatcher.manager.StoreExecutionSummary(ctx, string(wf.ID), report); err != nil {
		t.Fatalf("store execution summary: %v", err)
	}
	select {
	case evt := <-sink.C():
		t.Fatalf("unexpected summary telemetry during suppression: %s", evt.Kind)
	case <-time.After(50 * time.Millisecond):
	}

	if err := dispatcher.manager.WorkflowStatus(ctx, string(wf.ID), state.WorkflowFailed); err != nil {
		t.Fatalf("workflow status failed: %v", err)
	}
	evt := <-sink.C()
	if evt.Kind != telemetry.EventWorkflowFailed {
		t.Fatalf("expected workflow.failed after suppression cleared, got %s", evt.Kind)
	}
}

type errorQueue struct {
	err error
}

func (q *errorQueue) Enqueue(context.Context, workflow.Definition, state.Priority, map[string]any) (state.WorkflowID, error) {
	return "", nil
}

func (q *errorQueue) Claim(context.Context, state.Selector, string) (*state.ClaimedWorkflow, error) {
	return nil, q.err
}

func (q *errorQueue) Release(context.Context, state.WorkflowID) error { return nil }
func (q *errorQueue) Ack(context.Context, state.WorkflowID, state.ResultSummary) error {
	return nil
}
func (q *errorQueue) Cancel(context.Context, state.WorkflowID) error { return nil }
func (q *errorQueue) Stats(context.Context) (state.QueueStats, error) {
	return state.QueueStats{}, nil
}
func (q *errorQueue) PendingCount(context.Context, state.Selector) (int, error) {
	return 0, nil
}

func (q *errorQueue) AuditLog(context.Context, int) ([]state.QueueAuditRecord, error) {
	return nil, nil
}

func (q *errorQueue) Close() error { return nil }

type spyingStore struct {
	inner         state.Store
	summaryStored atomic.Bool
}

func newSpyingStore(inner state.Store) *spyingStore {
	return &spyingStore{inner: inner}
}

func (s *spyingStore) RecordWorkflow(ctx context.Context, rec state.WorkflowRecord) error {
	return s.inner.RecordWorkflow(ctx, rec)
}

func (s *spyingStore) UpdateWorkflowStatus(ctx context.Context, update state.WorkflowStatusUpdate) error {
	return s.inner.UpdateWorkflowStatus(ctx, update)
}

func (s *spyingStore) RecordStage(ctx context.Context, workflowID state.WorkflowID, stage state.StageRecord) error {
	return s.inner.RecordStage(ctx, workflowID, stage)
}

func (s *spyingStore) UpdateStageStatus(ctx context.Context, update state.StageStatusUpdate) error {
	return s.inner.UpdateStageStatus(ctx, update)
}

func (s *spyingStore) RecordAction(ctx context.Context, workflowID state.WorkflowID, stageID string, action state.ActionRecord) error {
	return s.inner.RecordAction(ctx, workflowID, stageID, action)
}

func (s *spyingStore) UpdateActionStatus(ctx context.Context, update state.ActionStatusUpdate) error {
	return s.inner.UpdateActionStatus(ctx, update)
}

func (s *spyingStore) StoreSummary(ctx context.Context, id state.WorkflowID, summary state.ResultSummary) error {
	s.summaryStored.Store(true)
	return s.inner.StoreSummary(ctx, id, summary)
}

func (s *spyingStore) WaitResult(ctx context.Context, id state.WorkflowID) (state.ResultSummary, error) {
	return s.inner.WaitResult(ctx, id)
}

func (s *spyingStore) Close() error {
	return s.inner.Close()
}

type spyQueue struct {
	inner          state.Queue
	store          *spyingStore
	ackBeforeStore atomic.Bool
	ackCount       atomic.Int32
}

func newSpyQueue(inner state.Queue, store *spyingStore) *spyQueue {
	return &spyQueue{
		inner: inner,
		store: store,
	}
}

func (q *spyQueue) Enqueue(ctx context.Context, def workflow.Definition, priority state.Priority, metadata map[string]any) (state.WorkflowID, error) {
	return q.inner.Enqueue(ctx, def, priority, metadata)
}

func (q *spyQueue) Claim(ctx context.Context, sel state.Selector, workerID string) (*state.ClaimedWorkflow, error) {
	return q.inner.Claim(ctx, sel, workerID)
}

func (q *spyQueue) Release(ctx context.Context, id state.WorkflowID) error {
	return q.inner.Release(ctx, id)
}

func (q *spyQueue) Ack(ctx context.Context, id state.WorkflowID, summary state.ResultSummary) error {
	if !q.store.summaryStored.Load() {
		q.ackBeforeStore.Store(true)
	}
	q.ackCount.Add(1)
	return q.inner.Ack(ctx, id, summary)
}

func (q *spyQueue) Cancel(ctx context.Context, id state.WorkflowID) error {
	return q.inner.Cancel(ctx, id)
}

func (q *spyQueue) Stats(ctx context.Context) (state.QueueStats, error) {
	return q.inner.Stats(ctx)
}

func (q *spyQueue) PendingCount(ctx context.Context, sel state.Selector) (int, error) {
	return q.inner.PendingCount(ctx, sel)
}

func (q *spyQueue) AuditLog(ctx context.Context, limit int) ([]state.QueueAuditRecord, error) {
	return q.inner.AuditLog(ctx, limit)
}

func (q *spyQueue) Close() error {
	return q.inner.Close()
}

func TestFailurePolicyRetryReleasesWorkflow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	registry.SetDefault(registry.NewSafeRegistry())

	var attempts int32
	requeue := make(chan struct{}, 1)

	MustRegisterAction("test.fail-once", func() ActionFunc {
		return func(actionCtx rt.Context) error {
			if atomic.AddInt32(&attempts, 1) == 1 {
				return errors.New("first attempt failed")
			}
			return nil
		}
	})

	def := workflow.Definition{
		ID: "wf-retry",
		Stages: []workflow.Stage{
			{
				Actions: []workflow.Action{{Ref: "test.fail-once"}},
			},
		},
	}
	if normalized, _, err := workflow.EnsureIDs(def); err != nil {
		t.Fatalf("ensure ids: %v", err)
	} else {
		def = normalized
	}

	queue := newRetryQueue(def, requeue)
	store := state.NewMemoryStore()
	manager, err := state.NewStoreManager(store)
	if err != nil {
		t.Fatalf("store manager: %v", err)
	}

	br := broker.NewLocal(manager)
	run := runner.New(local.Factory{}, registry.Default(), br)

	policy := FailurePolicyFunc(func(_ context.Context, info FailureContext) FailureOutcome {
		if info.Err != nil && info.Attempt < 2 {
			return RetryOutcome()
		}
		return AckOutcome()
	})

	pool := pools.NewLocal("local", state.Selector{}, 1)
	dispatcher := newDispatcher(ctx, queue, store, manager, run, nil, nil, nil, telemetry.NoopLogger{}, 5*time.Millisecond, 0, 1, policy, []*poolBinding{{pool: pool}})
	dispatcher.start()
	defer dispatcher.stop()

	select {
	case <-requeue:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected workflow to be requeued")
	}
}

type retryQueue struct {
	mu      sync.Mutex
	def     workflow.Definition
	pending []*state.ClaimedWorkflow
	requeue chan<- struct{}
}

func newRetryQueue(def workflow.Definition, requeue chan<- struct{}) *retryQueue {
	return &retryQueue{
		def: def,
		pending: []*state.ClaimedWorkflow{
			{
				QueuedWorkflow: state.QueuedWorkflow{
					ID:         state.WorkflowID("wf-retry"),
					Definition: def,
					Priority:   state.PriorityDefault,
					CreatedAt:  time.Now(),
				},
			},
		},
		requeue: requeue,
	}
}

func (q *retryQueue) Enqueue(context.Context, workflow.Definition, state.Priority, map[string]any) (state.WorkflowID, error) {
	return "", nil
}

func (q *retryQueue) Claim(context.Context, state.Selector, string) (*state.ClaimedWorkflow, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.pending) == 0 {
		return nil, state.ErrNoPending
	}
	item := q.pending[0]
	q.pending = q.pending[1:]
	item.Attempt++
	item.LeaseID = "lease"
	item.ClaimedAt = time.Now()
	return item, nil
}

func (q *retryQueue) Release(context.Context, state.WorkflowID) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending = append(q.pending, &state.ClaimedWorkflow{
		QueuedWorkflow: state.QueuedWorkflow{
			ID:         state.WorkflowID("wf-retry"),
			Definition: q.def,
			Priority:   state.PriorityDefault,
			CreatedAt:  time.Now(),
			Attempt:    1,
		},
	})
	if q.requeue != nil {
		q.requeue <- struct{}{}
	}
	return nil
}

func (q *retryQueue) Ack(context.Context, state.WorkflowID, state.ResultSummary) error { return nil }
func (q *retryQueue) Cancel(context.Context, state.WorkflowID) error                   { return nil }
func (q *retryQueue) Stats(context.Context) (state.QueueStats, error)                  { return state.QueueStats{}, nil }
func (q *retryQueue) PendingCount(context.Context, state.Selector) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending), nil
}

func (q *retryQueue) AuditLog(context.Context, int) ([]state.QueueAuditRecord, error) {
	return nil, nil
}

func (q *retryQueue) Close() error { return nil }

func TestDispatcherEmitsCancelledEventOnExplicitCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	registry.SetDefault(registry.NewSafeRegistry())

	started := make(chan struct{})
	var once sync.Once

	const actionID = "test.wait.cancel"
	MustRegisterAction(actionID, func() ActionFunc {
		return func(actionCtx rt.Context) error {
			once.Do(func() { close(started) })
			<-actionCtx.Done()
			return actionCtx.Err()
		}
	})

	def := workflow.Definition{
		ID: "wf-cancel",
		Stages: []workflow.Stage{
			{
				Name:    "wait",
				Actions: []workflow.Action{{Ref: actionID}},
			},
			{
				Name:    "after",
				Actions: []workflow.Action{{Ref: actionID}},
			},
		},
	}
	if normalized, _, err := workflow.EnsureIDs(def); err != nil {
		t.Fatalf("ensure ids: %v", err)
	} else {
		def = normalized
	}

	queue := state.NewMemoryQueue()
	id, err := queue.Enqueue(ctx, def, state.PriorityDefault, nil)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	store := state.NewMemoryStore()
	baseManager, err := state.NewStoreManager(store)
	if err != nil {
		t.Fatalf("store manager: %v", err)
	}

	teleDispatcher := node.NewTelemetryDispatcher(ctx, nil, node.TelemetryDispatcherConfig{})
	defer teleDispatcher.Close()

	sink := telemetry.NewChannelSink(64)
	unregister := teleDispatcher.Register(sink)
	defer unregister()

	manager := wrapWithTelemetry(baseManager, teleDispatcher)

	br := broker.NewLocal(manager)
	run := runner.New(local.Factory{}, registry.Default(), br)

	pool := pools.NewLocal("local", state.Selector{}, 1)
	dispatcher := newDispatcher(ctx, queue, store, manager, run, teleDispatcher, nil, nil, telemetry.NoopLogger{}, 5*time.Millisecond, 0, 0, nil, []*poolBinding{{pool: pool}})
	dispatcher.start()
	defer dispatcher.stop()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatalf("workflow did not start before timeout")
	}

	dispatcher.cancelWorkflow(id)
	_ = queue.Cancel(ctx, id)

	deadline := time.After(2 * time.Second)
	observed := map[telemetry.EventKind]bool{
		telemetry.EventWorkflowCancelRequest: false,
		telemetry.EventWorkflowCancelled:     false,
	}

	for {
		if observed[telemetry.EventWorkflowCancelRequest] && observed[telemetry.EventWorkflowCancelled] {
			break
		}
		select {
		case evt := <-sink.C():
			if evt.WorkflowID != string(id) {
				continue
			}
			switch evt.Kind {
			case telemetry.EventWorkflowCancelRequest:
				observed[telemetry.EventWorkflowCancelRequest] = true
			case telemetry.EventWorkflowCancelled:
				reason, ok := evt.Metadata["reason"].(string)
				if !ok || reason != "explicit_request" {
					continue
				}
				if poolName, ok := evt.Metadata["pool"].(string); !ok || poolName != pool.Name() {
					t.Fatalf("expected pool metadata %q, got %#v", pool.Name(), evt.Metadata["pool"])
				}
				observed[telemetry.EventWorkflowCancelled] = true
			}
		case <-deadline:
			t.Fatalf("telemetry events not observed: %+v", observed)
		}
	}
}
