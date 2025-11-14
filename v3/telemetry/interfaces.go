package telemetry

// Logger is a minimal structured logging interface used during workflow
// execution. Implementations can wrap the host application's logging stack.
type Logger interface {
	Debug(msg string, kv ...any)
	Info(msg string, kv ...any)
	Warn(msg string, kv ...any)
	Error(msg string, kv ...any)
}

// LogLevel represents the minimum log level for filtering messages.
type LogLevel int

const (
	// LogLevelDebug represents debug-level logging.
	LogLevelDebug LogLevel = iota
	// LogLevelInfo represents info-level logging.
	LogLevelInfo
	// LogLevelWarn represents warn-level logging.
	LogLevelWarn
	// LogLevelError represents error-level logging.
	LogLevelError
)

// Sink consumes telemetry events.
type Sink interface {
	Record(Event)
}

// Dispatcher manages telemetry event fan-out to registered sinks.
// Note: The concrete implementation is in node/telemetry.go as TelemetryDispatcher.
// This interface allows telemetry package to define the contract without
// depending on node package.
type Dispatcher interface {
	Dispatch(Event) error
	Register(Sink) func()
	Stats() interface{} // Returns TelemetryStats (defined in node package)
	Coverage(workflowID string) map[EventKind]int
	ClearCoverage(workflowID string)
	Close()
}
