package gostage

import "sync"

// MutationKind identifies the type of dynamic mutation.
type MutationKind string

const (
	// MutInsertAfter inserts a new step after the current one.
	MutInsertAfter MutationKind = "insert_after"
	// MutDisableStep disables a step by ID.
	MutDisableStep MutationKind = "disable"
	// MutEnableStep re-enables a disabled step.
	MutEnableStep MutationKind = "enable"
)

// Mutation represents a runtime workflow modification.
type Mutation struct {
	Kind     MutationKind `json:"kind"`
	TargetID string       `json:"target_id"`
	TaskName string       `json:"task_name,omitempty"`
}

// mutationQueue is a thread-safe queue of pending mutations.
type mutationQueue struct {
	mu       sync.Mutex
	pending  []Mutation
}

func newMutationQueue() *mutationQueue {
	return &mutationQueue{}
}

// Push adds a mutation to the queue. Safe for concurrent use.
func (q *mutationQueue) Push(m Mutation) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending = append(q.pending, m)
}

// Drain returns all pending mutations and clears the queue.
func (q *mutationQueue) Drain() []Mutation {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.pending) == 0 {
		return nil
	}
	out := q.pending
	q.pending = nil
	return out
}

// Len returns the number of pending mutations.
func (q *mutationQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}
