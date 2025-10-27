package state

import (
	"context"
	"time"

	"github.com/davidroman0O/gostage/v3/workflow"
)

type Priority int

const (
	PriorityDefault Priority = 0
	PriorityHigh    Priority = 10
	PriorityLow     Priority = -10
)

type Selector struct {
	All  []string
	Any  []string
	None []string
}

type WorkflowID string

type QueuedWorkflow struct {
	ID         WorkflowID
	Definition workflow.Definition
	Priority   Priority
	CreatedAt  time.Time
	Attempt    int
	Metadata   map[string]any
}

type ClaimedWorkflow struct {
	QueuedWorkflow
	ClaimedAt time.Time
	LeaseID   string
	WorkerID  string
}

type ResultSummary struct {
	Success         bool
	Error           string
	Attempt         int
	Output          map[string]any
	Duration        time.Duration
	CompletedAt     time.Time
	DisabledStages  map[string]bool
	DisabledActions map[string]bool
	RemovedStages   map[string]string
	RemovedActions  map[string]string
	Reason          TerminationReason

	StageStatuses  []StageStatusRecord
	ActionStatuses []ActionStatusRecord
}

// StageStatusRecord captures the terminal state of a stage at summary time.
type StageStatusRecord struct {
	ID          string
	Name        string
	Description string
	Tags        []string
	Dynamic     bool
	CreatedBy   string
	Status      WorkflowState
}

// ActionStatusRecord captures the terminal state of an action at summary time.
type ActionStatusRecord struct {
	StageID     string
	ActionID    string
	Name        string
	Description string
	Tags        []string
	Dynamic     bool
	CreatedBy   string
	Status      WorkflowState
}

type QueueStats struct {
	Pending   int
	Claimed   int
	Cancelled int
}

// QueueAuditRecord captures a queue lifecycle event for observability.
type QueueAuditRecord struct {
	WorkflowID WorkflowID
	Event      string
	WorkerID   string
	Attempt    int
	Metadata   map[string]any
	Timestamp  time.Time
}

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

type TerminationReason string

const (
	TerminationReasonUnknown      TerminationReason = "unknown"
	TerminationReasonSuccess      TerminationReason = "success"
	TerminationReasonFailure      TerminationReason = "failure"
	TerminationReasonUserCancel   TerminationReason = "user_cancel"
	TerminationReasonPolicyCancel TerminationReason = "policy_cancel"
	TerminationReasonTimeout      TerminationReason = "timeout"
)

type WorkflowRecord struct {
	ID                WorkflowID
	Name              string
	Description       string
	Type              string
	Tags              []string
	Metadata          map[string]any
	CreatedAt         time.Time
	State             WorkflowState
	StartedAt         *time.Time
	CompletedAt       *time.Time
	Duration          time.Duration
	Success           bool
	Error             string
	TerminationReason TerminationReason
	Definition        SubWorkflowDef
	Stages            map[string]*StageRecord
}

type WorkflowSummary struct {
	WorkflowRecord
}

type StateFilter struct {
	Tags   []string
	States []WorkflowState
	From   *time.Time
	To     *time.Time
	Limit  int
	Offset int
	Type   []string
}

type ActionHistoryRecord struct {
	ActionID    string
	StageID     string
	Ref         string
	Description string
	Tags        []string
	Dynamic     bool
	CreatedBy   string
	State       WorkflowState
	StartedAt   *time.Time
	CompletedAt *time.Time
	Progress    int
	Message     string
}

type SubWorkflowDef struct {
	ID        string
	Name      string
	Type      string
	Tags      []string
	Metadata  map[string]any
	Payload   map[string]any
	Priority  Priority
	CreatedAt time.Time
}

type StageRecord struct {
	ID          string
	Name        string
	Description string
	Tags        []string
	Dynamic     bool
	CreatedBy   string
	Status      WorkflowState
	Actions     map[string]*ActionRecord
	StartedAt   *time.Time
	CompletedAt *time.Time
}

type ActionRecord struct {
	Ref         string
	Name        string
	Description string
	Tags        []string
	Dynamic     bool
	CreatedBy   string
	Status      WorkflowState
	StartedAt   *time.Time
	CompletedAt *time.Time
}

type ExecutionReport struct {
	WorkflowID   string
	WorkflowName string
	WorkflowType string
	WorkflowTags []string
	Description  string
	Status       WorkflowState
	Success      bool
	ErrorMessage string
	Reason       TerminationReason
	StartedAt    time.Time
	CompletedAt  time.Time
	Duration     time.Duration
	FinalStore   map[string]any
	Attempt      int

	Stages          []StageSummary
	DisabledStages  map[string]bool
	DisabledActions map[string]bool
	RemovedStages   map[string]string
	RemovedActions  map[string]string
}

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
	Actions     []ActionSummary
}

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

// WorkflowStatusUpdate describes a targeted workflow status mutation.
type WorkflowStatusUpdate struct {
	ID          WorkflowID
	Status      WorkflowState
	StartedAt   *time.Time
	CompletedAt *time.Time
	Duration    *time.Duration
	Success     *bool
	Error       *string
	Reason      *TerminationReason
}

// StageStatusUpdate describes a stage state change.
type StageStatusUpdate struct {
	WorkflowID  WorkflowID
	StageID     string
	Status      WorkflowState
	StartedAt   *time.Time
	CompletedAt *time.Time
}

// ActionStatusUpdate describes an action state change.
type ActionStatusUpdate struct {
	WorkflowID  WorkflowID
	StageID     string
	ActionID    string
	Status      WorkflowState
	StartedAt   *time.Time
	CompletedAt *time.Time
}

type Manager interface {
	WorkflowRegistered(ctx context.Context, wf WorkflowRecord) error
	WorkflowStatus(ctx context.Context, workflowID string, status WorkflowState) error
	StageRegistered(ctx context.Context, workflowID string, stage StageRecord) error
	StageStatus(ctx context.Context, workflowID, stageID string, status WorkflowState) error
	ActionRegistered(ctx context.Context, workflowID, stageID string, action ActionRecord) error
	ActionStatus(ctx context.Context, workflowID, stageID, actionName string, status WorkflowState) error
	ActionProgress(ctx context.Context, workflowID, stageID, actionName string, progress int, message string) error
	ActionEvent(ctx context.Context, workflowID, stageID, actionName, kind, message string, metadata map[string]any) error
	ActionRemoved(ctx context.Context, workflowID, stageID, actionName, createdBy string) error
	StageRemoved(ctx context.Context, workflowID, stageID, createdBy string) error
	StoreExecutionSummary(ctx context.Context, workflowID string, report ExecutionReport) error
}
