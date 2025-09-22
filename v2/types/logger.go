package types

// Logger provides a simple interface for workflow logging
type Logger interface {
	// Debug logs a message at debug level
	Debug(format string, args ...interface{})

	// Info logs a message at info level
	Info(format string, args ...interface{})

	// Warn logs a message at warning level
	Warn(format string, args ...interface{})

	// Error logs a message at error level
	Error(format string, args ...interface{})
}

// Log level enumeration for structured logging
type LogLevel string

const (
	LogLevelInfo  LogLevel = "info"
	LogLevelWarn  LogLevel = "warn"
	LogLevelDebug LogLevel = "debug"
	LogLevelError LogLevel = "error"
)

// Structured log field for optional metadata
type LogField struct {
	Key   string
	Value interface{}
}
