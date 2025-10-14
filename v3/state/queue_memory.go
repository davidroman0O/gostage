package state

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/davidroman0O/gostage/v3/workflow"
	deadlock "github.com/sasha-s/go-deadlock"
)

// MemoryQueue provides an in-memory queue for tests.
type MemoryQueue struct {
	mu      deadlock.Mutex
	cond    *sync.Cond
	waiters map[string]chan ResultSummary
	items   []*memQueueItem
}

type memQueueItem struct {
	queued   QueuedWorkflow
	metadata map[string]any
}

// NewMemoryQueue creates a new MemoryQueue instance.
func NewMemoryQueue() *MemoryQueue {
	q := &MemoryQueue{
		items:   make([]*memQueueItem, 0),
		waiters: make(map[string]chan ResultSummary),
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *MemoryQueue) Enqueue(ctx context.Context, def workflow.Definition, priority Priority, metadata map[string]any) (WorkflowID, error) {
	q.mu.Lock()
	defer func() {
		q.mu.Unlock()
		q.cond.Signal()
	}()
	id := WorkflowID(uuid.NewString())
	entry := &memQueueItem{
		queued: QueuedWorkflow{
			ID:         id,
			Definition: def.Clone(),
			Priority:   priority,
			CreatedAt:  time.Now(),
			Metadata:   cloneAnyMap(metadata),
		},
	}
	entry.metadata = cloneAnyMap(metadata)
	q.items = append(q.items, entry)
	sort.SliceStable(q.items, func(i, j int) bool {
		a, b := q.items[i], q.items[j]
		if a.queued.Priority == b.queued.Priority {
			return a.queued.CreatedAt.Before(b.queued.CreatedAt)
		}
		return a.queued.Priority > b.queued.Priority
	})
	return id, nil
}

func (q *MemoryQueue) Claim(ctx context.Context, sel Selector, workerID string) (*ClaimedWorkflow, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return nil, ErrNoPending
	}
	item := q.items[0]
	q.items = q.items[1:]
	item.queued.Attempt++
	claimed := &ClaimedWorkflow{
		QueuedWorkflow: item.queued,
		ClaimedAt:      time.Now(),
		LeaseID:        uuid.NewString(),
		WorkerID:       workerID,
	}
	q.waiters[string(item.queued.ID)] = make(chan ResultSummary, 1)
	return claimed, nil
}

func (q *MemoryQueue) dequeueLocked(sel Selector) *memQueueItem {
	for idx, item := range q.items {
		if matchesSelector(item.queued.Definition.Tags, sel) {
			q.items = append(q.items[:idx], q.items[idx+1:]...)
			return item
		}
	}
	return nil
}

func (q *MemoryQueue) Release(ctx context.Context, id WorkflowID) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	// No-op: the runner may call Release when a claim cannot proceed.
	return nil
}

func (q *MemoryQueue) Ack(ctx context.Context, id WorkflowID, summary ResultSummary) error {
	q.mu.Lock()
	ch := q.waiters[string(id)]
	delete(q.waiters, string(id))
	q.mu.Unlock()
	if ch != nil {
		ch <- summary
	}
	return nil
}

func (q *MemoryQueue) Cancel(ctx context.Context, id WorkflowID) error {
	return q.Ack(ctx, id, ResultSummary{Success: false, Error: "cancelled"})
}

func (q *MemoryQueue) Stats(ctx context.Context) (QueueStats, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return QueueStats{Pending: len(q.items)}, nil
}

func (q *MemoryQueue) Close() error { return nil }
