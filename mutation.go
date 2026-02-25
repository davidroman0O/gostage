package gostage

import (
	"fmt"
	"strings"
	"sync"
)

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

// captureDynamicState snapshots the current workflow modifications as a
// replayable list of mutations. This captures:
//   - Disabled steps (as MutDisableStep)
//   - Dynamically inserted steps (as MutInsertAfter, identified by ":dyn:" in ID)
func captureDynamicState(wf *Workflow) []Mutation {
	var mutations []Mutation

	for i, s := range wf.steps {
		if s.disabled {
			mutations = append(mutations, Mutation{Kind: MutDisableStep, TargetID: s.id})
		}
		// Dynamically inserted steps have ":dyn:" in their ID
		if strings.Contains(s.id, ":dyn:") {
			anchorID := ""
			if i > 0 {
				anchorID = wf.steps[i-1].id
			}
			mutations = append(mutations, Mutation{
				Kind:     MutInsertAfter,
				TargetID: anchorID,
				TaskName: s.taskName,
			})
		}
	}

	return mutations
}

// replayMutations applies persisted mutations to a freshly cloned workflow.
// This restores dynamic modifications (inserted steps, disabled steps)
// that were captured before a suspend or sleep.
func replayMutations(wf *Workflow, mutations []Mutation) {
	for _, m := range mutations {
		switch m.Kind {
		case MutInsertAfter:
			td := lookupTask(m.TaskName)
			if td == nil {
				continue
			}
			wf.dynCounter++
			newStep := step{
				id:       fmt.Sprintf("%s:dyn:%d", wf.ID, wf.dynCounter),
				kind:     StepSingle,
				name:     m.TaskName,
				taskName: m.TaskName,
			}
			if m.TargetID == "" {
				// Insert at the beginning
				wf.steps = append([]step{newStep}, wf.steps...)
				continue
			}
			inserted := false
			for i, s := range wf.steps {
				if s.id == m.TargetID {
					pos := i + 1
					wf.steps = append(wf.steps, step{})
					copy(wf.steps[pos+1:], wf.steps[pos:])
					wf.steps[pos] = newStep
					inserted = true
					break
				}
			}
			if !inserted {
				// Anchor not found — append to end
				wf.steps = append(wf.steps, newStep)
			}
		case MutDisableStep:
			for i := range wf.steps {
				if wf.steps[i].id == m.TargetID || wf.steps[i].name == m.TargetID {
					wf.steps[i].disabled = true
				}
			}
		case MutEnableStep:
			for i := range wf.steps {
				if wf.steps[i].id == m.TargetID || wf.steps[i].name == m.TargetID {
					wf.steps[i].disabled = false
				}
			}
		}
	}
}
