package gostage

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

// DefaultLogger is a no-op logger implementation
type DefaultLogger struct{}

// Debug implements Logger.Debug
func (l *DefaultLogger) Debug(format string, args ...interface{}) {}

// Info implements Logger.Info
func (l *DefaultLogger) Info(format string, args ...interface{}) {}

// Warn implements Logger.Warn
func (l *DefaultLogger) Warn(format string, args ...interface{}) {}

// Error implements Logger.Error
func (l *DefaultLogger) Error(format string, args ...interface{}) {}

// NewDefaultLogger creates a new default no-op logger
func NewDefaultLogger() Logger {
	return &DefaultLogger{}
}
