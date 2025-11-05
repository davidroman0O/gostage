package gostage

import (
	"os"
	"path/filepath"

	rt "github.com/davidroman0O/gostage/v3/runtime"
	telemetrypkg "github.com/davidroman0O/gostage/v3/telemetry"
)

// CurrentBinary returns the absolute path to the running binary, falling back
// to the raw executable path if symlink resolution fails.
func CurrentBinary() string {
	path, err := os.Executable()
	if err != nil {
		return ""
	}
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		return resolved
	}
	return path
}

// EmitActionEvent emits a custom telemetry event associated with the current
// action. Metadata is copied to avoid mutation after the call.
// Deprecated: use telemetry.EmitActionEvent instead.
func EmitActionEvent(ctx rt.Context, kind, message string, metadata map[string]any) error {
	return telemetrypkg.EmitActionEvent(ctx, kind, message, metadata)
}
