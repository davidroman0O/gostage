package telemetry

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestPrometheusSinkCountsEventsAndOutcomes(t *testing.T) {
	reg := prometheus.NewRegistry()
	sink := NewPrometheusSink(reg)
	ps, ok := sink.(*PromSink)
	if !ok {
		t.Fatalf("unexpected sink type %T", sink)
	}

	// Record a success workflow summary
	sink.Record(Event{Kind: EventWorkflowSummary, Metadata: map[string]any{"success": true, "duration": float64(0.5)}})

	// Events counter increments for the summary kind
	if v := testutil.ToFloat64(ps.events.WithLabelValues(string(EventWorkflowSummary))); v != 1 {
		t.Fatalf("expected events counter 1, got %v", v)
	}

	// Outcome counter increments for success
	if v := testutil.ToFloat64(ps.outcomes.WithLabelValues("success")); v != 1 {
		t.Fatalf("expected outcomes success counter 1, got %v", v)
	}
}
