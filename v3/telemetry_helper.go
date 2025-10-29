package gostage

import (
	rt "github.com/davidroman0O/gostage/v3/runtime"
	telemetrypkg "github.com/davidroman0O/gostage/v3/telemetry"
)

// EmitActionEvent emits a custom telemetry event associated with the current
// action. Metadata is copied to avoid mutation after the call.
// Deprecated: use telemetry.EmitActionEvent instead.
func EmitActionEvent(ctx rt.Context, kind, message string, metadata map[string]any) error {
	return telemetrypkg.EmitActionEvent(ctx, kind, message, metadata)
}
