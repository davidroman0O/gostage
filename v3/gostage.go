package gostage

import (
	"context"

	"github.com/davidroman0O/gostage/v3/orchestrator"
)

type (
	WorkflowID                 = orchestrator.WorkflowID
	WorkflowRecord             = orchestrator.WorkflowRecord
	StateReader                = orchestrator.StateReader
	StateFilter                = orchestrator.StateFilter
	WorkflowSummary            = orchestrator.WorkflowSummary
	ActionHistoryRecord        = orchestrator.ActionHistoryRecord
	WorkflowState              = orchestrator.WorkflowState
	StageRecord                = orchestrator.StageRecord
	ActionRecord               = orchestrator.ActionRecord
	ResultSummary              = orchestrator.ResultSummary
	StateObserver              = orchestrator.StateObserver
	StateObserverFuncs         = orchestrator.StateObserverFuncs
	TelemetryDispatcherConfig  = orchestrator.TelemetryDispatcherConfig
	TelemetryStats             = orchestrator.TelemetryStats
	OverflowStrategy           = orchestrator.OverflowStrategy
	TerminationReason          = orchestrator.TerminationReason
	Selector                   = orchestrator.Selector
	PoolConfig                 = orchestrator.PoolConfig
	TLSFiles                   = orchestrator.TLSFiles
	RemoteBridgeConfig         = orchestrator.RemoteBridgeConfig
	SpawnerConfig              = orchestrator.SpawnerConfig
	DispatcherConfig           = orchestrator.DispatcherConfig
	SQLiteConfig               = orchestrator.SQLiteConfig
	ChildMetadataConflict      = orchestrator.ChildMetadataConflict
	ChildMetadataConflictError = orchestrator.ChildMetadataConflictError
)

const (
	WorkflowPending   = orchestrator.WorkflowPending
	WorkflowClaimed   = orchestrator.WorkflowClaimed
	WorkflowRunning   = orchestrator.WorkflowRunning
	WorkflowCompleted = orchestrator.WorkflowCompleted
	WorkflowFailed    = orchestrator.WorkflowFailed
	WorkflowCancelled = orchestrator.WorkflowCancelled
	WorkflowSkipped   = orchestrator.WorkflowSkipped
	WorkflowRemoved   = orchestrator.WorkflowRemoved

	OverflowStrategyDropOldest = orchestrator.OverflowStrategyDropOldest
	OverflowStrategyBlock      = orchestrator.OverflowStrategyBlock
	OverflowStrategyFailFast   = orchestrator.OverflowStrategyFailFast

	TerminationReasonUnknown      = orchestrator.TerminationReasonUnknown
	TerminationReasonSuccess      = orchestrator.TerminationReasonSuccess
	TerminationReasonFailure      = orchestrator.TerminationReasonFailure
	TerminationReasonUserCancel   = orchestrator.TerminationReasonUserCancel
	TerminationReasonPolicyCancel = orchestrator.TerminationReasonPolicyCancel
	TerminationReasonTimeout      = orchestrator.TerminationReasonTimeout
)

var (
	ErrNodeClosed             = orchestrator.ErrNodeClosed
	ErrNoMatchingPool         = orchestrator.ErrNoMatchingPool
	ErrDuplicatePool          = orchestrator.ErrDuplicatePool
	ErrInvalidPoolConfig      = orchestrator.ErrInvalidPoolConfig
	ErrDuplicateSpawner       = orchestrator.ErrDuplicateSpawner
	ErrUnknownSpawner         = orchestrator.ErrUnknownSpawner
	ErrInvalidSpawnerConfig   = orchestrator.ErrInvalidSpawnerConfig
	ErrSubmissionRejected     = orchestrator.ErrSubmissionRejected
	ErrRemoteMissingAuthToken = orchestrator.ErrMissingAuthToken
	ErrRemoteMissingTLSPair   = orchestrator.ErrMissingTLSPair
	ErrRemoteMissingTLSCA     = orchestrator.ErrMissingTLSCA
)

type (
	Node             = orchestrator.Node
	Result           = orchestrator.Result
	Snapshot         = orchestrator.Snapshot
	PoolSnapshot     = orchestrator.PoolSnapshot
	ChildNode        = orchestrator.ChildNode
	DiagnosticEvent  = orchestrator.DiagnosticEvent
	TelemetryHandler = orchestrator.TelemetryHandler
	HealthEvent      = orchestrator.HealthEvent
	HealthHandler    = orchestrator.HealthHandler
	CancelFunc       = orchestrator.CancelFunc
	ChildHandler     = orchestrator.ChildHandler
)

func Run(ctx context.Context, opts ...Option) (*Node, <-chan DiagnosticEvent, error) {
	return orchestrator.Run(ctx, opts...)
}

func IsChildProcess() bool { return orchestrator.IsChildProcess() }

func HandleChild(handler ChildHandler, opts ...ChildOption) {
	orchestrator.HandleChild(handler, opts...)
}

func HandleChildNamed(childType string, handler ChildHandler, opts ...ChildOption) {
	orchestrator.HandleChildNamed(childType, handler, opts...)
}

func poolMetadataToStrings(values map[string]any) map[string]string {
	return orchestrator.PoolMetadataToStrings(values)
}
