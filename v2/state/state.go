package state

import (
	"context"
	"time"
)

type Priority int

const (
	PriorityDefault Priority = 0
	PriorityHigh    Priority = 10
	PriorityLow     Priority = -10
)

type WorkflowState string

const (
	WorkflowPending   WorkflowState = "pending"
	WorkflowClaimed   WorkflowState = "claimed"
	WorkflowRunning   WorkflowState = "running"
	WorkflowCompleted WorkflowState = "completed"
	WorkflowFailed    WorkflowState = "failed"
	WorkflowCancelled WorkflowState = "cancelled"
	WorkflowSkipped   WorkflowState = "skipped"
	WorkflowRemoved   WorkflowState = "removed"
)

type WorkerStatus string

const (
	WorkerIdle    WorkerStatus = "idle"
	WorkerBusy    WorkerStatus = "busy"
	WorkerOffline WorkerStatus = "offline"
)

type SubWorkflowDef struct {
	ID          string
	Name        string
	Type        string
	Tags        []string
	Payload     map[string]interface{}
	Metadata    map[string]interface{}
	Priority    Priority
	CreatedAt   time.Time
	ScheduledAt time.Time
}

type WorkflowFilter struct {
	Types []string
	Tags  []string
}

type WorkflowQuery struct {
	States []WorkflowState
	Types  []string
	Tags   []string
}

type WorkflowResult struct {
	Success bool
	Error   string
	Output  map[string]interface{}
	EndedAt time.Time
}

type StageRecord struct {
	ID          string
	Name        string
	Description string
	Tags        []string
	Dynamic     bool
	CreatedBy   string
	Status      WorkflowState
	StartedAt   time.Time
	EndedAt     time.Time
	Actions     map[string]*ActionRecord
	RemovedBy   string
}

type ActionRecord struct {
	Name        string
	Description string
	Tags        []string
	Dynamic     bool
	CreatedBy   string
	Status      WorkflowState
	Progress    int
	Message     string
	StartedAt   time.Time
	EndedAt     time.Time
	RemovedBy   string
}

// ExecutionReport captures a full workflow run summary for archival and introspection.
type ExecutionReport struct {
	WorkflowID   string
	WorkflowName string
	WorkflowType string
	WorkflowTags []string
	Description  string
	Status       WorkflowState
	Success      bool
	ErrorMessage string
	StartedAt    time.Time
	CompletedAt  time.Time
	Duration     time.Duration
	FinalStore   map[string]interface{}

	Stages          []StageSummary
	DisabledStages  map[string]bool
	DisabledActions map[string]bool
	RemovedStages   map[string]string
	RemovedActions  map[string]string
}

// StageSummary provides per-stage telemetry in an execution report.
type StageSummary struct {
	ID          string
	Name        string
	Description string
	Tags        []string
	Dynamic     bool
	CreatedBy   string
	Status      WorkflowState
	Disabled    bool
	RemovedBy   string
	StartedAt   time.Time
	EndedAt     time.Time
	Actions     []ActionSummary
}

// ActionSummary provides per-action telemetry in an execution report.
type ActionSummary struct {
	StageID     string
	Name        string
	Description string
	Tags        []string
	Dynamic     bool
	CreatedBy   string
	Status      WorkflowState
	Disabled    bool
	RemovedBy   string
	Progress    int
	Message     string
	StartedAt   time.Time
	EndedAt     time.Time
}

type WorkflowStatus struct {
	ID         string
	Definition SubWorkflowDef
	State      WorkflowState
	WorkerID   string
	WorkerType string
	ClaimedAt  time.Time
	StartedAt  time.Time
	EndedAt    time.Time
	Result     *WorkflowResult
	Stages     map[string]*StageRecord
}

type QueuedWorkflow struct {
	Status WorkflowStatus
}

type ClaimedWorkflow struct {
	Status WorkflowStatus
}

type WorkerState struct {
	ID         string
	Type       string
	Tags       []string
	Status     WorkerStatus
	Metadata   map[string]interface{}
	LastBeatAt time.Time
}

type EventType string

const (
	EventWorkflowStored  EventType = "workflow_stored"
	EventWorkflowClaimed EventType = "workflow_claimed"
	EventWorkflowUpdated EventType = "workflow_updated"
)

type Event struct {
	Type    EventType
	Time    time.Time
	Payload interface{}
}

type Manager interface {
	Close()

	WorkflowRegistered(ctx context.Context, wf WorkflowRecord) error
	WorkflowStatus(ctx context.Context, workflowID string, status WorkflowState) error
	StageRegistered(ctx context.Context, workflowID string, stage StageRecord) error
	StageStatus(ctx context.Context, workflowID, stageID string, status WorkflowState) error
	ActionRegistered(ctx context.Context, workflowID, stageID string, action ActionRecord) error
	ActionStatus(ctx context.Context, workflowID, stageID, actionName string, status WorkflowState) error
	ActionProgress(ctx context.Context, workflowID, stageID, actionName string, progress int, message string) error
	ActionRemoved(ctx context.Context, workflowID, stageID, actionName, createdBy string) error
	StageRemoved(ctx context.Context, workflowID, stageID, createdBy string) error
	StoreExecutionSummary(ctx context.Context, workflowID string, report ExecutionReport) error
	GetExecutionSummary(ctx context.Context, workflowID string) (*ExecutionReport, error)

	Store(def SubWorkflowDef) string
	StoreWithPriority(def SubWorkflowDef, priority Priority) string
	ClaimWorkflow(workerID, workerType string, filter WorkflowFilter) (*QueuedWorkflow, error)
	ReleaseWorkflow(workflowID string) error
	CompleteWorkflow(workflowID string, result *WorkflowResult) error
	CancelWorkflow(workflowID string) error
	Pull(filter WorkflowFilter) (*QueuedWorkflow, error)

	GetStatus(workflowID string) (*WorkflowStatus, error)
	WaitForCompletion(workflowID string, timeout time.Duration) (*WorkflowResult, error)
	QueryWorkflows(query WorkflowQuery) []*WorkflowStatus
	GetWorkflowCounts() map[WorkflowState]int
	ListPendingWorkflows() []*QueuedWorkflow
	ListClaimedWorkflows() []*ClaimedWorkflow

	RegisterWorker(workerID, workerType string, tags []string, metadata map[string]interface{}) error
	UnregisterWorker(workerID string) error
	WorkerHeartbeat(workerID string) error
	UpdateWorkerState(workerID string, status WorkerStatus) error
	ListWorkers() []*WorkerState
	GetWorkerCounts() map[WorkerStatus]int

	WaitForWork(workerType string, timeout time.Duration) error

	AddEventHandler(eventType EventType, handler func(Event))
	RemoveEventHandlers(eventType EventType)

	GenerateWorkflowID() string
}

type WorkflowRecord struct {
	ID          string
	Name        string
	Description string
	Tags        []string
	State       WorkflowState
	Definition  SubWorkflowDef
	Stages      map[string]*StageRecord
}
