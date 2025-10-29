package telemetry

import (
	"errors"

	rt "github.com/davidroman0O/gostage/v3/runtime"
)

// EmitActionEvent emits a custom telemetry event associated with the current action.
// Metadata is copied to avoid mutation after the call.
func EmitActionEvent(ctx rt.Context, kind, message string, metadata map[string]any) error {
	if ctx == nil {
		return errors.New("telemetry: runtime context required")
	}
	broker := ctx.Broker()
	if broker == nil {
		return nil
	}
	var meta map[string]any
	if len(metadata) > 0 {
		meta = make(map[string]any, len(metadata))
		for k, v := range metadata {
			meta[k] = v
		}
	}
	return broker.Event(kind, message, meta)
}
