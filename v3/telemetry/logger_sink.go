package telemetry

import "context"

// LoggingSink forwards telemetry events to a Logger.
type LoggingSink struct {
	logger Logger
	ctx    context.Context
}

func NewLoggingSink(ctx context.Context, logger Logger) *LoggingSink {
	if ctx == nil {
		ctx = context.Background()
	}
	if logger == nil {
		logger = NoopLogger{}
	}
	return &LoggingSink{logger: logger, ctx: ctx}
}

func (s *LoggingSink) Record(evt Event) {
	s.logger.Info("telemetry",
		"kind", evt.Kind,
		"workflow", evt.WorkflowID,
		"stage", evt.StageID,
		"action", evt.ActionID,
		"attempt", evt.Attempt,
		"message", evt.Message,
		"error", evt.Err,
	)
}
