package telemetry

import "log"

// Logger is defined in interfaces.go.
// NoopLogger and StdLogger provide implementations.

// NoopLogger drops all log messages. Useful during tests.
type NoopLogger struct{}

func (NoopLogger) Debug(string, ...any) {}
func (NoopLogger) Info(string, ...any)  {}
func (NoopLogger) Warn(string, ...any)  {}
func (NoopLogger) Error(string, ...any) {}

// StdLogger wraps the standard library logger.
type StdLogger struct {
	log *log.Logger
}

func NewStdLogger(l *log.Logger) StdLogger {
	if l == nil {
		l = log.Default()
	}
	return StdLogger{log: l}
}

func (s StdLogger) Debug(msg string, kv ...any) { s.log.Printf("DEBUG: %s %v", msg, kv) }
func (s StdLogger) Info(msg string, kv ...any)  { s.log.Printf("INFO: %s %v", msg, kv) }
func (s StdLogger) Warn(msg string, kv ...any)  { s.log.Printf("WARN: %s %v", msg, kv) }
func (s StdLogger) Error(msg string, kv ...any) { s.log.Printf("ERROR: %s %v", msg, kv) }
