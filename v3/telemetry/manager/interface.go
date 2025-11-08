package manager

import (
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

// Suppressor provides methods to suppress telemetry events for workflows.
type Suppressor interface {
	SuppressWorkflowEvents(workflowID string, kinds ...telemetry.EventKind)
}

// ManagerWithSuppression extends state.Manager with suppression capabilities.
type ManagerWithSuppression interface {
	state.Manager
	Suppressor
}

