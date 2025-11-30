package spawner

import (
	"context"

	"github.com/davidroman0O/gostage/v3/internal/infrastructure/diagnostics"
)

// Reporter allows the spawner to emit diagnostic events.
type Reporter interface {
	Report(diagnostics.Event)
}

// Spawner launches child processes for remote workflow execution.
// ProcessSpawner is the concrete implementation.
// Note: Launch returns *ProcessHandle (concrete type) for backward compatibility.
type Spawner interface {
	// Launch starts a child process with the provided launch configuration and returns a handle.
	Launch(ctx context.Context, launch LaunchConfig) (*ProcessHandle, error)

	// SetReporter updates the diagnostics reporter used for child processes.
	SetReporter(rep Reporter)

	// Reporter returns the currently configured diagnostics reporter.
	Reporter() Reporter
}
