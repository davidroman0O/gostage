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
