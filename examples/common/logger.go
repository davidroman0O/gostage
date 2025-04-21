package common

import (
	"fmt"
	"strings"
	"time"

	"github.com/davidroman0O/gostage"
)

// LogLevel represents different logging levels
type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

// ConsoleLogger is a simple console logger implementation
type ConsoleLogger struct {
	level LogLevel
}

// NewConsoleLogger creates a new console logger with the specified log level
func NewConsoleLogger(level LogLevel) gostage.Logger {
	return &ConsoleLogger{
		level: level,
	}
}

// Debug implements Logger.Debug
func (l *ConsoleLogger) Debug(format string, args ...interface{}) {
	if l.level <= LogLevelDebug {
		l.log("DEBUG", format, args...)
	}
}

// Info implements Logger.Info
func (l *ConsoleLogger) Info(format string, args ...interface{}) {
	if l.level <= LogLevelInfo {
		l.log("INFO", format, args...)
	}
}

// Warn implements Logger.Warn
func (l *ConsoleLogger) Warn(format string, args ...interface{}) {
	if l.level <= LogLevelWarn {
		l.log("WARN", format, args...)
	}
}

// Error implements Logger.Error
func (l *ConsoleLogger) Error(format string, args ...interface{}) {
	if l.level <= LogLevelError {
		l.log("ERROR", format, args...)
	}
}

// log formats and prints a log message to the console
func (l *ConsoleLogger) log(level string, format string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05.000")
	// Pad the level string to ensure consistent formatting
	paddedLevel := fmt.Sprintf("%-5s", level)

	// If there are indentation characters at the start, preserve them
	indentation := ""
	if strings.HasPrefix(format, "  ") {
		indentation = strings.Repeat("  ", strings.Count(format, "  "))
		format = strings.TrimLeft(format, " ")
	}

	// Format the message
	message := fmt.Sprintf(format, args...)

	// Print the formatted log message
	fmt.Printf("%s [%s] %s%s\n", timestamp, paddedLevel, indentation, message)
}
