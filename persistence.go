package gostage

import (
	"context"
	"encoding/json"
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

	// DeleteRun removes a run and all its associated state.
	DeleteRun(ctx context.Context, runID RunID) error

	// UpdateCurrentStep updates only the current step identifier for a run.
	UpdateCurrentStep(ctx context.Context, runID RunID, stepID string) error

	// ListRuns returns runs matching the given filter.
	ListRuns(ctx context.Context, filter RunFilter) ([]*RunState, error)

	// Close releases any resources held by the persistence layer.
	Close() error
}

// InMemoryPersistence is a marker interface for persistence implementations
// that do not survive process restarts. When the engine detects an in-memory
// backend, sleep steps block the goroutine instead of scheduling a timer-based
// wake — since there is nothing durable to persist, the scheduler cannot
// restore the workflow after a restart.
type InMemoryPersistence interface {
	Persistence
	isInMemory()
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
	DynCounter  int               `json:"dyn_counter,omitempty"`
	WorkflowDef *WorkflowDef     `json:"workflow_def,omitempty"`
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
	// Offset skips the first N results. Used with Limit for pagination.
	Offset int
	// Before filters by UpdatedAt < this time. Zero means no time filter.
	Before time.Time
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

// SaveRun stores a deep copy of the run state in memory.
func (m *memoryPersistence) SaveRun(_ context.Context, run *RunState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runs[run.RunID] = copyRunState(run)
	return nil
}

// LoadRun retrieves a deep copy of a run by its ID.
// Returns a RunNotFoundError if the run does not exist.
func (m *memoryPersistence) LoadRun(_ context.Context, runID RunID) (*RunState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	run, ok := m.runs[runID]
	if !ok {
		return nil, &RunNotFoundError{RunID: runID}
	}
	return copyRunState(run), nil
}

// UpdateStepStatus updates the status of a specific step within an in-memory run.
// Returns a RunNotFoundError if the run does not exist.
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

// SaveState merges the given state entries into the in-memory store for a run.
// Existing keys are overwritten; new keys are added.
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

// LoadState retrieves a copy of all state entries for a run.
// Returns nil if no state entries exist for the given run.
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

// DeleteState removes all state entries for a run from the in-memory store.
func (m *memoryPersistence) DeleteState(_ context.Context, runID RunID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.states, runID)
	return nil
}

// DeleteStateKey removes a single state entry for a run from the in-memory store.
func (m *memoryPersistence) DeleteStateKey(_ context.Context, runID RunID, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.states[runID] != nil {
		delete(m.states[runID], key)
	}
	return nil
}

// DeleteRun removes a run and all its associated state from the in-memory store.
func (m *memoryPersistence) DeleteRun(_ context.Context, runID RunID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.runs, runID)
	delete(m.states, runID)
	return nil
}

// UpdateCurrentStep updates the current step identifier for an in-memory run.
// Returns a RunNotFoundError if the run does not exist.
func (m *memoryPersistence) UpdateCurrentStep(_ context.Context, runID RunID, stepID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	run, ok := m.runs[runID]
	if !ok {
		return &RunNotFoundError{RunID: runID}
	}
	run.CurrentStep = stepID
	run.UpdatedAt = time.Now()
	return nil
}

// isInMemory implements the InMemoryPersistence marker interface.
func (m *memoryPersistence) isInMemory() {}

// ListRuns returns deep copies of runs matching the given filter criteria.
// Results are filtered by workflow ID, status, and update time, then paginated
// with offset and limit.
func (m *memoryPersistence) ListRuns(_ context.Context, filter RunFilter) ([]*RunState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var filtered []*RunState
	for _, run := range m.runs {
		if filter.WorkflowID != "" && run.WorkflowID != filter.WorkflowID {
			continue
		}
		if filter.Status != "" && run.Status != filter.Status {
			continue
		}
		if !filter.Before.IsZero() && !run.UpdatedAt.Before(filter.Before) {
			continue
		}
		filtered = append(filtered, copyRunState(run))
	}
	// Apply offset and limit.
	start := filter.Offset
	if start < 0 {
		start = 0
	}
	if start >= len(filtered) {
		return nil, nil
	}
	filtered = filtered[start:]
	if filter.Limit > 0 && len(filtered) > filter.Limit {
		filtered = filtered[:filter.Limit]
	}
	return filtered, nil
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

	if run.WorkflowDef != nil {
		data, err := json.Marshal(run.WorkflowDef)
		if err == nil {
			var defCopy WorkflowDef
			if json.Unmarshal(data, &defCopy) == nil {
				cp.WorkflowDef = &defCopy
			}
		}
	}

	return &cp
}

// Close is a no-op for the in-memory persistence implementation.
func (m *memoryPersistence) Close() error {
	return nil
}

// RunNotFoundError indicates a run was not found.
type RunNotFoundError struct {
	RunID RunID
}

// Error returns a human-readable message including the missing run ID.
func (e *RunNotFoundError) Error() string {
	return "run not found: " + string(e.RunID)
}

// Is reports whether target matches the sentinel ErrRunNotFound, enabling
// errors.Is(err, ErrRunNotFound) to work with typed RunNotFoundError values.
func (e *RunNotFoundError) Is(target error) bool {
	return target == ErrRunNotFound
}
