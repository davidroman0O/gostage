package telemetry

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// PromSink publishes workflow telemetry as Prometheus metrics.
//
// Metrics exposed:
//   - gostage_events_total: Counter of events by kind
//   - gostage_workflow_outcomes_total: Counter of workflow outcomes by result
//   - gostage_workflow_duration_seconds: Histogram of workflow durations
//   - gostage_workflow_outcomes_by_type_total: Counter of outcomes by workflow type
//   - gostage_pool_workflows_total: Counter of workflows processed by pool
//   - gostage_pool_errors_total: Counter of errors by pool
//   - gostage_stage_duration_seconds: Histogram of stage durations
//   - gostage_action_duration_seconds: Histogram of action durations
//   - gostage_telemetry_buffer_utilization: Gauge of telemetry buffer utilization
//   - gostage_telemetry_events_dropped_total: Counter of dropped telemetry events
//
// Usage:
//
//	reg := prometheus.DefaultRegisterer
//	sink := telemetry.NewPrometheusSink(reg)
//	gostage.Run(ctx, gostage.WithTelemetrySink(sink))
type PromSink struct {
	events            *prometheus.CounterVec
	outcomes          *prometheus.CounterVec
	outcomesByType    *prometheus.CounterVec
	durations         prometheus.Observer
	poolWorkflows     *prometheus.CounterVec
	poolErrors        *prometheus.CounterVec
	stageDurations    prometheus.Observer
	actionDurations   prometheus.Observer
	bufferUtilization prometheus.Gauge
	eventsDropped     prometheus.Counter
}

// NewPrometheusSink constructs a metrics sink and registers it with the provided Registerer.
func NewPrometheusSink(reg prometheus.Registerer) Sink {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	events := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gostage_events_total",
		Help: "Total telemetry events by kind.",
	}, []string{"kind"})
	outcomes := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gostage_workflow_outcomes_total",
		Help: "Workflow outcomes by result (success,failure,cancel,skip).",
	}, []string{"outcome"})
	outcomesByType := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gostage_workflow_outcomes_by_type_total",
		Help: "Workflow outcomes by workflow type and result.",
	}, []string{"workflow_type", "outcome"})
	duration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "gostage_workflow_duration_seconds",
		Help:    "Observed workflow durations (from summary events).",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 16),
	})
	poolWorkflows := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gostage_pool_workflows_total",
		Help: "Total workflows processed by pool.",
	}, []string{"pool"})
	poolErrors := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gostage_pool_errors_total",
		Help: "Total errors by pool.",
	}, []string{"pool"})
	stageDuration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "gostage_stage_duration_seconds",
		Help:    "Observed stage execution durations.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
	})
	actionDuration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "gostage_action_duration_seconds",
		Help:    "Observed action execution durations.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
	})
	bufferUtilization := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gostage_telemetry_buffer_utilization",
		Help: "Current telemetry buffer utilization (0.0 to 1.0).",
	})
	eventsDropped := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "gostage_telemetry_events_dropped_total",
		Help: "Total telemetry events dropped due to backpressure.",
	})

	reg.MustRegister(events, outcomes, outcomesByType, duration, poolWorkflows, poolErrors,
		stageDuration, actionDuration, bufferUtilization, eventsDropped)
	return &PromSink{
		events:            events,
		outcomes:          outcomes,
		outcomesByType:    outcomesByType,
		durations:         duration,
		poolWorkflows:     poolWorkflows,
		poolErrors:        poolErrors,
		stageDurations:    stageDuration,
		actionDurations:   actionDuration,
		bufferUtilization: bufferUtilization,
		eventsDropped:     eventsDropped,
	}
}

// Record records a telemetry event and updates Prometheus metrics.
func (s *PromSink) Record(evt Event) {
	if s == nil {
		return
	}
	s.events.WithLabelValues(string(evt.Kind)).Inc()

	// Extract pool from metadata if available
	pool := ""
	workflowType := ""
	if evt.Metadata != nil {
		if p, ok := evt.Metadata[MetadataKeyPool].(string); ok {
			pool = p
		}
		if wt, ok := evt.Metadata["workflow_type"].(string); ok {
			workflowType = wt
		}
	}

	switch evt.Kind {
	case EventWorkflowSummary:
		// Outcome from metadata: success(bool) or reason(string)
		outcome := "failure"
		if evt.Metadata != nil {
			if v, ok := evt.Metadata[MetadataKeyReason].(string); ok && v != "" {
				lower := strings.ToLower(v)
				switch lower {
				case "success":
					outcome = "success"
				case "user_cancel", "policy_cancel", "cancel", "cancelled", "canceled":
					outcome = "cancel"
				case "skipped", "skip":
					outcome = "skip"
				default:
					outcome = "failure"
				}
			} else if success, ok := evt.Metadata[MetadataKeySuccess].(bool); ok && success {
				outcome = "success"
			}
			if dur, ok := evt.Metadata[MetadataKeyDuration].(float64); ok && dur > 0 {
				// duration stored as seconds in summaries
				s.durations.Observe(dur)
			}
		}
		s.outcomes.WithLabelValues(outcome).Inc()
		if workflowType != "" {
			s.outcomesByType.WithLabelValues(workflowType, outcome).Inc()
		}
		if pool != "" {
			s.poolWorkflows.WithLabelValues(pool).Inc()
		}
	case EventWorkflowFailed, EventWorkflowCancelled:
		if pool != "" {
			s.poolErrors.WithLabelValues(pool).Inc()
		}
	case EventStageCompleted, EventStageFailed:
		if evt.Metadata != nil {
			if dur, ok := evt.Metadata[MetadataKeyDuration].(float64); ok && dur > 0 {
				s.stageDurations.Observe(dur)
			}
		}
	case EventActionCompleted, EventActionFailed:
		if evt.Metadata != nil {
			if dur, ok := evt.Metadata[MetadataKeyDuration].(float64); ok && dur > 0 {
				s.actionDurations.Observe(dur)
			}
		}
	}
}

// UpdateBufferUtilization updates the buffer utilization gauge.
// This should be called periodically from the telemetry dispatcher.
func (s *PromSink) UpdateBufferUtilization(utilization float64) {
	if s != nil && s.bufferUtilization != nil {
		s.bufferUtilization.Set(utilization)
	}
}

// RecordDroppedEvent increments the dropped events counter.
func (s *PromSink) RecordDroppedEvent() {
	if s != nil && s.eventsDropped != nil {
		s.eventsDropped.Inc()
	}
}
