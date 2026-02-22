package gostage

import (
	"context"
	"time"
)

// Persistence defines the generic interface for workflow run persistence.
// Implementations handle storing and retrieving workflow execution state.
// The engine uses this to checkpoint after each step and support resume.
type Persistence interface {
	// SaveRun persists a new or updated run state.
	SaveRun(ctx context.Context, run *RunState) error

	// LoadRun retrieves a run by its ID.
	LoadRun(ctx context.Context, runID RunID) (*RunState, error)

	// UpdateStepStatus updates the status of a specific step within a run.
	UpdateStepStatus(ctx context.Context, runID RunID, stepID string, status Status) error

	// SaveCheckpoint persists the store snapshot for a run.
	// This is called after each step to enable resume from the last checkpoint.
	SaveCheckpoint(ctx context.Context, runID RunID, storeData []byte) error

	// LoadCheckpoint retrieves the last store snapshot for a run.
	LoadCheckpoint(ctx context.Context, runID RunID) ([]byte, error)

	// ListRuns returns runs matching the given filter.
	ListRuns(ctx context.Context, filter RunFilter) ([]*RunState, error)

	// Close releases any resources held by the persistence layer.
	Close() error
}

// RunState represents the persisted state of a workflow execution.
type RunState struct {
	RunID      RunID             `json:"run_id"`
	WorkflowID string           `json:"workflow_id"`
	Status     Status            `json:"status"`
	CurrentStep string           `json:"current_step"`
	StepStates map[string]Status `json:"step_states"`
	BailReason string            `json:"bail_reason,omitempty"`
	SuspendData map[string]any   `json:"suspend_data,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
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
type memoryPersistence struct {
	runs        map[RunID]*RunState
	checkpoints map[RunID][]byte
}

func newMemoryPersistence() *memoryPersistence {
	return &memoryPersistence{
		runs:        make(map[RunID]*RunState),
		checkpoints: make(map[RunID][]byte),
	}
}

func (m *memoryPersistence) SaveRun(_ context.Context, run *RunState) error {
	m.runs[run.RunID] = run
	return nil
}

func (m *memoryPersistence) LoadRun(_ context.Context, runID RunID) (*RunState, error) {
	run, ok := m.runs[runID]
	if !ok {
		return nil, &RunNotFoundError{RunID: runID}
	}
	return run, nil
}

func (m *memoryPersistence) UpdateStepStatus(_ context.Context, runID RunID, stepID string, status Status) error {
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

func (m *memoryPersistence) SaveCheckpoint(_ context.Context, runID RunID, storeData []byte) error {
	m.checkpoints[runID] = storeData
	return nil
}

func (m *memoryPersistence) LoadCheckpoint(_ context.Context, runID RunID) ([]byte, error) {
	data, ok := m.checkpoints[runID]
	if !ok {
		return nil, &RunNotFoundError{RunID: runID}
	}
	return data, nil
}

func (m *memoryPersistence) ListRuns(_ context.Context, filter RunFilter) ([]*RunState, error) {
	var results []*RunState
	for _, run := range m.runs {
		if filter.WorkflowID != "" && run.WorkflowID != filter.WorkflowID {
			continue
		}
		if filter.Status != "" && run.Status != filter.Status {
			continue
		}
		results = append(results, run)
		if filter.Limit > 0 && len(results) >= filter.Limit {
			break
		}
	}
	return results, nil
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
