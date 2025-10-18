package state

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/davidroman0O/gostage/v3/internal/locks"
	"github.com/davidroman0O/gostage/v3/workflow"
)

// MemoryQueue provides an in-memory queue for tests.
type MemoryQueue struct {
	mu       locks.Mutex
	cond     *sync.Cond
	waiters  map[string]chan ResultSummary
	items    []*memQueueItem
	inflight map[string]*memQueueItem
	audit    []QueueAuditRecord
}

type memQueueItem struct {
	queued   QueuedWorkflow
	metadata map[string]any
}

const memoryQueueAuditLimit = DefaultAuditLogLimit

// NewMemoryQueue creates a new MemoryQueue instance.
func NewMemoryQueue() *MemoryQueue {
	q := &MemoryQueue{
		items:    make([]*memQueueItem, 0),
		waiters:  make(map[string]chan ResultSummary),
		inflight: make(map[string]*memQueueItem),
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
	index := -1
	for i, item := range q.items {
		if matchesSelector(item.queued.Definition.Tags, sel) {
			index = i
			break
		}
	}
	if index == -1 {
		return nil, ErrNoPending
	}
	item := q.items[index]
	q.items = append(q.items[:index], q.items[index+1:]...)
	item.queued.Attempt++
	claimed := &ClaimedWorkflow{
		QueuedWorkflow: item.queued,
		ClaimedAt:      time.Now(),
		LeaseID:        uuid.NewString(),
		WorkerID:       workerID,
	}
	workflowID := string(item.queued.ID)
	q.waiters[workflowID] = make(chan ResultSummary, 1)
	q.inflight[workflowID] = item
	q.recordAuditLocked("claim", item.queued.ID, item.queued.Attempt, workerID, map[string]any{
		"selector": map[string][]string{
			"all":  append([]string(nil), sel.All...),
			"any":  append([]string(nil), sel.Any...),
			"none": append([]string(nil), sel.None...),
		},
	})
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
	key := string(id)
	item, ok := q.inflight[key]
	if !ok {
		return nil
	}
	delete(q.inflight, key)
	q.items = append(q.items, item)
	sort.SliceStable(q.items, func(i, j int) bool {
		a, b := q.items[i], q.items[j]
		if a.queued.Priority == b.queued.Priority {
			return a.queued.CreatedAt.Before(b.queued.CreatedAt)
		}
		return a.queued.Priority > b.queued.Priority
	})
	q.recordAuditLocked("release", id, item.queued.Attempt, "", nil)
	q.cond.Signal()
	return nil
}

func (q *MemoryQueue) Ack(ctx context.Context, id WorkflowID, summary ResultSummary) error {
	key := string(id)
	q.mu.Lock()
	item := q.inflight[key]
	ch := q.waiters[key]
	delete(q.waiters, key)
	delete(q.inflight, key)
	q.mu.Unlock()
	if item != nil && summary.Attempt == 0 {
		summary.Attempt = item.queued.Attempt
	}
	q.mu.Lock()
	q.recordAuditLocked("ack", id, summary.Attempt, "", nil)
	q.mu.Unlock()
	if ch != nil {
		ch <- summary
	}
	return nil
}

func (q *MemoryQueue) Cancel(_ context.Context, id WorkflowID) error {
	key := string(id)
	q.mu.Lock()
	if _, ok := q.inflight[key]; ok {
		q.recordAuditLocked("cancel", id, 0, "", map[string]any{
			"state": "inflight",
		})
		q.mu.Unlock()
		return nil
	}
	for idx, item := range q.items {
		if item.queued.ID == id {
			q.items = append(q.items[:idx], q.items[idx+1:]...)
			q.recordAuditLocked("cancel", id, item.queued.Attempt, "", map[string]any{
				"state": "pending",
			})
			q.mu.Unlock()
			q.cond.Signal()
			return nil
		}
	}
	q.mu.Unlock()
	return ErrNoPending
}

func (q *MemoryQueue) Stats(ctx context.Context) (QueueStats, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return QueueStats{Pending: len(q.items), Claimed: len(q.inflight)}, nil
}

func (q *MemoryQueue) PendingCount(ctx context.Context, sel Selector) (int, error) {
	_ = ctx
	q.mu.Lock()
	defer q.mu.Unlock()
	count := 0
	for _, item := range q.items {
		if matchesSelector(item.queued.Definition.Tags, sel) {
			count++
		}
	}
	return count, nil
}

func (q *MemoryQueue) AuditLog(ctx context.Context, limit int) ([]QueueAuditRecord, error) {
	_ = ctx
	q.mu.Lock()
	defer q.mu.Unlock()
	if limit <= 0 || limit > len(q.audit) {
		limit = len(q.audit)
	}
	result := make([]QueueAuditRecord, limit)
	for i := 0; i < limit; i++ {
		rec := q.audit[len(q.audit)-1-i]
		result[i] = cloneAuditRecord(rec)
	}
	return result, nil
}

func (q *MemoryQueue) Close() error { return nil }

func (q *MemoryQueue) recordAuditLocked(event string, id WorkflowID, attempt int, workerID string, metadata map[string]any) {
	if metadata != nil {
		metadata = cloneAnyMap(metadata)
	}
	rec := QueueAuditRecord{
		WorkflowID: id,
		Event:      event,
		WorkerID:   workerID,
		Attempt:    attempt,
		Metadata:   metadata,
		Timestamp:  time.Now(),
	}
	if len(q.audit) >= memoryQueueAuditLimit {
		copy(q.audit, q.audit[1:])
		q.audit[memoryQueueAuditLimit-1] = rec
	} else {
		q.audit = append(q.audit, rec)
	}
}

func cloneAuditRecord(rec QueueAuditRecord) QueueAuditRecord {
	out := rec
	if rec.Metadata != nil {
		out.Metadata = cloneAnyMap(rec.Metadata)
	}
	return out
}
