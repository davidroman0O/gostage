package gostage

// Logger provides a simple interface for structured workflow logging.
type Logger interface {
	// Debug logs a message at debug level.
	Debug(format string, args ...interface{})

	// Info logs a message at info level.
	Info(format string, args ...interface{})

	// Warn logs a message at warning level.
	Warn(format string, args ...interface{})

	// Error logs a message at error level.
	Error(format string, args ...interface{})
}

// DefaultLogger is a no-op Logger implementation that silently discards all messages.
type DefaultLogger struct{}

// Debug discards the message. Implements Logger.
func (l *DefaultLogger) Debug(format string, args ...interface{}) {}

// Info discards the message. Implements Logger.
func (l *DefaultLogger) Info(format string, args ...interface{}) {}

// Warn discards the message. Implements Logger.
func (l *DefaultLogger) Warn(format string, args ...interface{}) {}

// Error discards the message. Implements Logger.
func (l *DefaultLogger) Error(format string, args ...interface{}) {}

// NewDefaultLogger creates a new no-op logger that silently discards all messages.
func NewDefaultLogger() Logger {
	return &DefaultLogger{}
}
