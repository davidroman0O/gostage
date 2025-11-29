// Package testkit provides fault injection utilities for testing state management components.
package testing

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/davidroman0O/gostage/v3/internal/domain"
	"github.com/davidroman0O/gostage/v3/internal/domain/queue"
	"github.com/davidroman0O/gostage/v3/internal/domain/store"
	"github.com/davidroman0O/gostage/v3/internal/foundation/clock"
)

// QueueFaults controls injected delays and failures for a queue wrapper.
type QueueFaults struct {
	DelayEnqueue time.Duration
	DelayClaim   time.Duration
	DelayRelease time.Duration
	DelayAck     time.Duration
	DelayCancel  time.Duration

	FailRelease int // number of initial Release calls to fail
	FailAck     int // number of initial Ack calls to fail
}

// ErrInjectedFailure is returned by faulty wrappers when configured to fail.
var ErrInjectedFailure = errors.New("testkit: injected failure")

// FaultyQueue wraps a domain.Queue and injects delays/failures as configured.
type FaultyQueue struct {
	inner domain.Queue
	mu    sync.Mutex
	cfg   QueueFaults
}

// NewFaultyQueue creates a new FaultyQueue wrapper around the given queue.
func NewFaultyQueue(inner domain.Queue, cfg QueueFaults) *FaultyQueue {
	if inner == nil {
		inner = domain.NewQueueAdapter(queue.NewMemoryQueue(clock.DefaultClock()))
	}
	return &FaultyQueue{inner: inner, cfg: cfg}
}

// Enqueue enqueues a workflow with optional delay.
func (q *FaultyQueue) Enqueue(ctx context.Context, defBytes []byte, priority domain.Priority, metadata map[string]any) (domain.WorkflowID, error) {
	if d := q.cfg.DelayEnqueue; d > 0 {
		time.Sleep(d)
	}
	return q.inner.Enqueue(ctx, defBytes, priority, metadata)
}

// Claim claims a workflow with optional delay.
func (q *FaultyQueue) Claim(ctx context.Context, sel domain.Selector, workerID string) (*domain.ClaimedWorkflow, error) {
	if d := q.cfg.DelayClaim; d > 0 {
		time.Sleep(d)
	}
	return q.inner.Claim(ctx, sel, workerID)
}

// Release releases a workflow with optional delay and failure injection.
func (q *FaultyQueue) Release(ctx context.Context, id domain.WorkflowID) error {
	if d := q.cfg.DelayRelease; d > 0 {
		time.Sleep(d)
	}
	q.mu.Lock()
	if q.cfg.FailRelease > 0 {
		q.cfg.FailRelease--
		q.mu.Unlock()
		return ErrInjectedFailure
	}
	q.mu.Unlock()
	return q.inner.Release(ctx, id)
}

// Ack acknowledges a workflow completion with optional delay and failure injection.
func (q *FaultyQueue) Ack(ctx context.Context, id domain.WorkflowID, summary domain.ResultSummary) error {
	if d := q.cfg.DelayAck; d > 0 {
		time.Sleep(d)
	}
	q.mu.Lock()
	if q.cfg.FailAck > 0 {
		q.cfg.FailAck--
		q.mu.Unlock()
		return ErrInjectedFailure
	}
	q.mu.Unlock()
	return q.inner.Ack(ctx, id, summary)
}

// Cancel cancels a workflow with optional delay.
func (q *FaultyQueue) Cancel(ctx context.Context, id domain.WorkflowID) error {
	if d := q.cfg.DelayCancel; d > 0 {
		time.Sleep(d)
	}
	return q.inner.Cancel(ctx, id)
}

// Stats returns queue statistics.
func (q *FaultyQueue) Stats(ctx context.Context) (domain.QueueStats, error) {
	return q.inner.Stats(ctx)
}

// PendingCount returns the count of pending workflows.
func (q *FaultyQueue) PendingCount(ctx context.Context, sel domain.Selector) (int, error) {
	return q.inner.PendingCount(ctx, sel)
}

// AuditLog returns the audit log.
func (q *FaultyQueue) AuditLog(ctx context.Context, limit int) ([]domain.QueueAuditRecord, error) {
	return q.inner.AuditLog(ctx, limit)
}

// Close closes the underlying queue.
func (q *FaultyQueue) Close() error { return q.inner.Close() }

// StoreFaults controls injected delays/failures for a store wrapper.
type StoreFaults struct {
	DelayRecordWorkflow time.Duration
	DelayUpdateWorkflow time.Duration
	DelayRecordStage    time.Duration
	DelayUpdateStage    time.Duration
	DelayRecordAction   time.Duration
	DelayUpdateAction   time.Duration
	DelayStoreSummary   time.Duration
	DelayWaitResult     time.Duration

	FailStoreSummary int // number of initial StoreSummary calls to fail
}

// FaultyStore wraps a domain.Store to inject delays/failures.
type FaultyStore struct {
	inner domain.Store
	mu    sync.Mutex
	cfg   StoreFaults
}

// NewFaultyStore creates a new faulty store wrapper with injected delays and failures.
func NewFaultyStore(inner domain.Store, cfg StoreFaults) *FaultyStore {
	if inner == nil {
		inner = domain.NewStoreAdapter(store.NewMemoryStore())
	}
	return &FaultyStore{inner: inner, cfg: cfg}
}

// RecordWorkflow records a workflow with optional delay injection.
func (s *FaultyStore) RecordWorkflow(ctx context.Context, rec domain.WorkflowRecord) error {
	if d := s.cfg.DelayRecordWorkflow; d > 0 {
		time.Sleep(d)
	}
	return s.inner.RecordWorkflow(ctx, rec)
}

// UpdateWorkflowStatus updates workflow status with optional delay injection.
func (s *FaultyStore) UpdateWorkflowStatus(ctx context.Context, update domain.WorkflowStatusUpdate) error {
	if d := s.cfg.DelayUpdateWorkflow; d > 0 {
		time.Sleep(d)
	}
	return s.inner.UpdateWorkflowStatus(ctx, update)
}

// RecordStage records a stage with optional delay injection.
func (s *FaultyStore) RecordStage(ctx context.Context, workflowID domain.WorkflowID, stage domain.StageRecord) error {
	if d := s.cfg.DelayRecordStage; d > 0 {
		time.Sleep(d)
	}
	return s.inner.RecordStage(ctx, workflowID, stage)
}

// UpdateStageStatus updates stage status with optional delay injection.
func (s *FaultyStore) UpdateStageStatus(ctx context.Context, update domain.StageStatusUpdate) error {
	if d := s.cfg.DelayUpdateStage; d > 0 {
		time.Sleep(d)
	}
	return s.inner.UpdateStageStatus(ctx, update)
}

// RecordAction records an action with optional delay injection.
func (s *FaultyStore) RecordAction(ctx context.Context, workflowID domain.WorkflowID, stageID string, action domain.ActionRecord) error {
	if d := s.cfg.DelayRecordAction; d > 0 {
		time.Sleep(d)
	}
	return s.inner.RecordAction(ctx, workflowID, stageID, action)
}

// UpdateActionStatus updates action status with optional delay injection.
func (s *FaultyStore) UpdateActionStatus(ctx context.Context, update domain.ActionStatusUpdate) error {
	if d := s.cfg.DelayUpdateAction; d > 0 {
		time.Sleep(d)
	}
	return s.inner.UpdateActionStatus(ctx, update)
}

// StoreSummary stores a workflow summary with optional delay and failure injection.
func (s *FaultyStore) StoreSummary(ctx context.Context, id domain.WorkflowID, summary domain.ResultSummary) error {
	if d := s.cfg.DelayStoreSummary; d > 0 {
		time.Sleep(d)
	}
	s.mu.Lock()
	if s.cfg.FailStoreSummary > 0 {
		s.cfg.FailStoreSummary--
		s.mu.Unlock()
		return ErrInjectedFailure
	}
	s.mu.Unlock()
	return s.inner.StoreSummary(ctx, id, summary)
}

// WaitResult waits for a workflow result with optional delay injection.
func (s *FaultyStore) WaitResult(ctx context.Context, id domain.WorkflowID) (domain.ResultSummary, error) {
	if d := s.cfg.DelayWaitResult; d > 0 {
		time.Sleep(d)
	}
	return s.inner.WaitResult(ctx, id)
}

// Close closes the underlying store.
func (s *FaultyStore) Close() error { return s.inner.Close() }
