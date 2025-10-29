package manager

import (
	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/orchestrator"
	"github.com/davidroman0O/gostage/v3/state"
)

// Wrap returns a manager that emits telemetry events via the provided dispatcher.
func Wrap(delegate state.Manager, dispatcher *node.TelemetryDispatcher) state.Manager {
	return orchestrator.WrapWithTelemetry(delegate, dispatcher)
}
