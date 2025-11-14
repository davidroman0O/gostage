// Package manager provides telemetry-aware wrappers for state management.
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
//
//nolint:revive // exported: ManagerWithSuppression is a well-established public API name
type ManagerWithSuppression interface {
	state.Manager
	Suppressor
}
