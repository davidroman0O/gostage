package telemetry

// Field represents a key-value pair for structured logging.
type Field struct {
	Key   string
	Value any
}

// Field creates a Field with the given key and value.
func NewField(key string, value any) Field {
	return Field{Key: key, Value: value}
}

// StructuredLogger extends Logger with structured field support.
// Implementations can format fields as key-value pairs or JSON.
type StructuredLogger interface {
	Logger
	// LogInfo logs an info message with structured fields.
	LogInfo(message string, fields ...Field)
	// LogError logs an error message with structured fields.
	LogError(err error, message string, fields ...Field)
	// LogDebug logs a debug message with structured fields.
	LogDebug(message string, fields ...Field)
	// LogWarn logs a warning message with structured fields.
	LogWarn(message string, fields ...Field)
}

// structuredLoggerAdapter adapts a Logger to StructuredLogger by formatting fields.
type structuredLoggerAdapter struct {
	Logger
}

// NewStructuredLogger wraps a Logger to provide structured logging methods.
func NewStructuredLogger(logger Logger) StructuredLogger {
	if logger == nil {
		return &structuredLoggerAdapter{Logger: NoopLogger{}}
	}
	if sl, ok := logger.(StructuredLogger); ok {
		return sl
	}
	return &structuredLoggerAdapter{Logger: logger}
}

func (a *structuredLoggerAdapter) LogInfo(message string, fields ...Field) {
	kv := fieldsToKV(fields)
	a.Info(message, kv...)
}

func (a *structuredLoggerAdapter) LogError(err error, message string, fields ...Field) {
	kv := fieldsToKV(fields)
	if err != nil {
		kv = append(kv, "error", err)
	}
	a.Error(message, kv...)
}

func (a *structuredLoggerAdapter) LogDebug(message string, fields ...Field) {
	kv := fieldsToKV(fields)
	a.Debug(message, kv...)
}

func (a *structuredLoggerAdapter) LogWarn(message string, fields ...Field) {
	kv := fieldsToKV(fields)
	a.Warn(message, kv...)
}

func fieldsToKV(fields []Field) []any {
	if len(fields) == 0 {
		return nil
	}
	kv := make([]any, 0, len(fields)*2)
	for _, f := range fields {
		kv = append(kv, f.Key, f.Value)
	}
	return kv
}

