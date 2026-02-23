package gostage

import (
	"context"
	"sync"
	"time"
)

// Persistence defines the generic interface for workflow run persistence.
// Implementations handle storing and retrieving workflow execution state.
// The engine uses this to flush state after each step and support resume.
type Persistence interface {
	// SaveRun persists a new or updated run state.
	SaveRun(ctx context.Context, run *RunState) error

	// LoadRun retrieves a run by its ID.
	LoadRun(ctx context.Context, runID RunID) (*RunState, error)

	// UpdateStepStatus updates the status of a specific step within a run.
	UpdateStepStatus(ctx context.Context, runID RunID, stepID string, status Status) error

	// SaveState persists dirty state entries for a run (per-key granularity).
	// Each entry is a JSON-encoded value with its Go type name for round-trip fidelity.
	SaveState(ctx context.Context, runID RunID, entries map[string]StateEntry) error

	// LoadState retrieves all state entries for a run.
	LoadState(ctx context.Context, runID RunID) (map[string]StateEntry, error)

	// DeleteState removes all state entries for a run.
	DeleteState(ctx context.Context, runID RunID) error

	// DeleteStateKey removes a single state entry for a run.
	DeleteStateKey(ctx context.Context, runID RunID, key string) error

	// ListRuns returns runs matching the given filter.
	ListRuns(ctx context.Context, filter RunFilter) ([]*RunState, error)

	// Close releases any resources held by the persistence layer.
	Close() error
}

// RunState represents the persisted state of a workflow execution.
type RunState struct {
	RunID       RunID             `json:"run_id"`
	WorkflowID  string            `json:"workflow_id"`
	Status      Status            `json:"status"`
	CurrentStep string            `json:"current_step"`
	StepStates  map[string]Status `json:"step_states"`
	BailReason  string            `json:"bail_reason,omitempty"`
	SuspendData map[string]any    `json:"suspend_data,omitempty"`
	WakeAt      time.Time         `json:"wake_at,omitempty"`
	Mutations   []Mutation        `json:"mutations,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

// RunFilter specifies criteria for listing runs.
type RunFilter struct {
	// WorkflowID filters by workflow ID. Empty means all workflows.
	WorkflowID string
	// Status filters by run status. Empty means all statuses.
	Status Status
	// Limit caps the number of results. 0 means no limit.
	Limit int
}

// memoryPersistence is an in-memory implementation of Persistence.
// Used as the default when no SQLite database is configured.
// Thread-safe via RWMutex for concurrent workflow execution.
type memoryPersistence struct {
	mu     sync.RWMutex
	runs   map[RunID]*RunState
	states map[RunID]map[string]StateEntry
}

func newMemoryPersistence() *memoryPersistence {
	return &memoryPersistence{
		runs:   make(map[RunID]*RunState),
		states: make(map[RunID]map[string]StateEntry),
	}
}

func (m *memoryPersistence) SaveRun(_ context.Context, run *RunState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runs[run.RunID] = copyRunState(run)
	return nil
}

func (m *memoryPersistence) LoadRun(_ context.Context, runID RunID) (*RunState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	run, ok := m.runs[runID]
	if !ok {
		return nil, &RunNotFoundError{RunID: runID}
	}
	return copyRunState(run), nil
}

func (m *memoryPersistence) UpdateStepStatus(_ context.Context, runID RunID, stepID string, status Status) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	run, ok := m.runs[runID]
	if !ok {
		return &RunNotFoundError{RunID: runID}
	}
	if run.StepStates == nil {
		run.StepStates = make(map[string]Status)
	}
	run.StepStates[stepID] = status
	run.UpdatedAt = time.Now()
	return nil
}

func (m *memoryPersistence) SaveState(_ context.Context, runID RunID, entries map[string]StateEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	existing, ok := m.states[runID]
	if !ok {
		existing = make(map[string]StateEntry, len(entries))
		m.states[runID] = existing
	}
	for k, v := range entries {
		existing[k] = v
	}
	return nil
}

func (m *memoryPersistence) LoadState(_ context.Context, runID RunID) (map[string]StateEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entries, ok := m.states[runID]
	if !ok {
		return nil, nil
	}
	// Return a copy
	out := make(map[string]StateEntry, len(entries))
	for k, v := range entries {
		out[k] = v
	}
	return out, nil
}

func (m *memoryPersistence) DeleteState(_ context.Context, runID RunID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.states, runID)
	return nil
}

func (m *memoryPersistence) DeleteStateKey(_ context.Context, runID RunID, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.states[runID] != nil {
		delete(m.states[runID], key)
	}
	return nil
}

func (m *memoryPersistence) ListRuns(_ context.Context, filter RunFilter) ([]*RunState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var results []*RunState
	for _, run := range m.runs {
		if filter.WorkflowID != "" && run.WorkflowID != filter.WorkflowID {
			continue
		}
		if filter.Status != "" && run.Status != filter.Status {
			continue
		}
		results = append(results, copyRunState(run))
		if filter.Limit > 0 && len(results) >= filter.Limit {
			break
		}
	}
	return results, nil
}

// copyRunState returns a shallow copy of run with its mutable fields deep-copied
// to prevent aliasing between callers and the stored record.
func copyRunState(run *RunState) *RunState {
	if run == nil {
		return nil
	}
	cp := *run // copy scalar fields

	if run.StepStates != nil {
		cp.StepStates = make(map[string]Status, len(run.StepStates))
		for k, v := range run.StepStates {
			cp.StepStates[k] = v
		}
	}

	if run.Mutations != nil {
		cp.Mutations = make([]Mutation, len(run.Mutations))
		copy(cp.Mutations, run.Mutations)
	}

	if run.SuspendData != nil {
		cp.SuspendData = make(map[string]any, len(run.SuspendData))
		for k, v := range run.SuspendData {
			cp.SuspendData[k] = v
		}
	}

	return &cp
}

func (m *memoryPersistence) Close() error {
	return nil
}

// RunNotFoundError indicates a run was not found.
type RunNotFoundError struct {
	RunID RunID
}

func (e *RunNotFoundError) Error() string {
	return "run not found: " + string(e.RunID)
}
