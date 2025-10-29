package gostage

import (
	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/state"
	telemetrymanager "github.com/davidroman0O/gostage/v3/telemetry/manager"
)

func wrapWithTelemetry(delegate state.Manager, dispatcher *node.TelemetryDispatcher) state.Manager {
	return telemetrymanager.Wrap(delegate, dispatcher)
}
