package runtime

// LogField represents a key-value pair for structured logging in runtime context.
// This is a runtime-local type to avoid import cycles with telemetry package.
type LogField struct {
	Key   string
	Value any
}

// NewLogField creates a LogField with the given key and value.
func NewLogField(key string, value any) LogField {
	return LogField{Key: key, Value: value}
}

// LoggingHelpers provides context-aware logging helpers for runtime Context.
// These helpers automatically include workflow, stage, and action context in log messages.
type LoggingHelpers struct {
	ctx Context
}

// NewLoggingHelpers creates logging helpers for the given context.
func NewLoggingHelpers(ctx Context) *LoggingHelpers {
	return &LoggingHelpers{ctx: ctx}
}

// LogInfo logs an info message with automatic context inclusion.
// Context includes: workflow_id, stage_id, action_id, attempt (if available).
func (h *LoggingHelpers) LogInfo(message string, fields ...LogField) {
	if h.ctx == nil {
		return
	}
	logger := h.ctx.Logger()
	if logger == nil {
		return
	}

	// Build context fields
	ctxFields := h.buildContextFields()
	allFields := make([]LogField, 0, len(ctxFields)+len(fields))
	allFields = append(allFields, ctxFields...)
	allFields = append(allFields, fields...)

	// Convert to key-value pairs for Logger interface
	kv := fieldsToKV(allFields)
	logger.Info(message, kv...)
}

// LogError logs an error message with automatic context inclusion.
func (h *LoggingHelpers) LogError(err error, message string, fields ...LogField) {
	if h.ctx == nil {
		return
	}
	logger := h.ctx.Logger()
	if logger == nil {
		return
	}

	ctxFields := h.buildContextFields()
	if err != nil {
		ctxFields = append(ctxFields, NewLogField("error", err.Error()))
	}
	allFields := make([]LogField, 0, len(ctxFields)+len(fields))
	allFields = append(allFields, ctxFields...)
	allFields = append(allFields, fields...)

	kv := fieldsToKV(allFields)
	logger.Error(message, kv...)
}

// LogDebug logs a debug message with automatic context inclusion.
func (h *LoggingHelpers) LogDebug(message string, fields ...LogField) {
	if h.ctx == nil {
		return
	}
	logger := h.ctx.Logger()
	if logger == nil {
		return
	}

	ctxFields := h.buildContextFields()
	allFields := make([]LogField, 0, len(ctxFields)+len(fields))
	allFields = append(allFields, ctxFields...)
	allFields = append(allFields, fields...)

	kv := fieldsToKV(allFields)
	logger.Debug(message, kv...)
}

// LogWarn logs a warning message with automatic context inclusion.
func (h *LoggingHelpers) LogWarn(message string, fields ...LogField) {
	if h.ctx == nil {
		return
	}
	logger := h.ctx.Logger()
	if logger == nil {
		return
	}

	ctxFields := h.buildContextFields()
	allFields := make([]LogField, 0, len(ctxFields)+len(fields))
	allFields = append(allFields, ctxFields...)
	allFields = append(allFields, fields...)

	kv := fieldsToKV(allFields)
	logger.Warn(message, kv...)
}

// buildContextFields extracts workflow/stage/action context from the context.
func (h *LoggingHelpers) buildContextFields() []LogField {
	fields := make([]LogField, 0, 4)

	if h.ctx.Workflow() != nil {
		fields = append(fields, NewLogField("workflow_id", h.ctx.Workflow().ID()))
		if h.ctx.Workflow().Name() != "" {
			fields = append(fields, NewLogField("workflow_name", h.ctx.Workflow().Name()))
		}
	}

	if h.ctx.Stage() != nil {
		fields = append(fields, NewLogField("stage_id", h.ctx.Stage().ID()))
		if h.ctx.Stage().Name() != "" {
			fields = append(fields, NewLogField("stage_name", h.ctx.Stage().Name()))
		}
	}

	if h.ctx.Action() != nil {
		fields = append(fields, NewLogField("action_name", h.ctx.Action().Name()))
		fields = append(fields, NewLogField("action_index", h.ctx.ActionIndex()))
	}

	return fields
}

func fieldsToKV(fields []LogField) []interface{} {
	if len(fields) == 0 {
		return nil
	}
	kv := make([]interface{}, 0, len(fields)*2)
	for _, f := range fields {
		kv = append(kv, f.Key, f.Value)
	}
	return kv
}

// LogInfo is a convenience function that creates LoggingHelpers and logs an info message.
func LogInfo(ctx Context, message string, fields ...LogField) {
	NewLoggingHelpers(ctx).LogInfo(message, fields...)
}

// LogError is a convenience function that creates LoggingHelpers and logs an error message.
func LogError(ctx Context, err error, message string, fields ...LogField) {
	NewLoggingHelpers(ctx).LogError(err, message, fields...)
}

// LogDebug is a convenience function that creates LoggingHelpers and logs a debug message.
func LogDebug(ctx Context, message string, fields ...LogField) {
	NewLoggingHelpers(ctx).LogDebug(message, fields...)
}

// LogWarn is a convenience function that creates LoggingHelpers and logs a warning message.
func LogWarn(ctx Context, message string, fields ...LogField) {
	NewLoggingHelpers(ctx).LogWarn(message, fields...)
}
