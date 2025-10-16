package telemetry

import (
	"context"
	"log"
)

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

// NewLoggerSink adapts a standard library logger to a telemetry Sink with a
// background context, matching the draft usage helper.
func NewLoggerSink(std *log.Logger) *LoggingSink {
	return NewLoggingSink(context.Background(), NewStdLogger(std))
}

func (s *LoggingSink) Record(evt Event) {
	s.logger.Info("telemetry",
		"kind", string(evt.Kind),
		"workflow", evt.WorkflowID,
		"stage", evt.StageID,
		"action", evt.ActionID,
		"attempt", evt.Attempt,
		"message", evt.Message,
		"error", evt.Error,
	)
}
