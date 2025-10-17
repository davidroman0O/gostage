package gostage

import (
	"context"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

func TestParentNodeStreamTelemetryWithoutDispatcher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	base := node.New(ctx, nil)
	defer func() {
		if err := base.Close(); err != nil {
			t.Fatalf("close base: %v", err)
		}
	}()

	parent := &parentNode{
		base: base,
	}

	events := make(chan telemetry.Event, 1)
	cancelStream := parent.StreamTelemetry(ctx, func(evt telemetry.Event) {
		select {
		case events <- evt:
		default:
		}
	})
	defer cancelStream()

	base.TelemetryDispatcher().Dispatch(telemetry.Event{
		Kind:       telemetry.EventWorkflowRegistered,
		WorkflowID: "wf-1",
	})

	select {
	case evt := <-events:
		if evt.WorkflowID != "wf-1" {
			t.Fatalf("unexpected workflow id %q", evt.WorkflowID)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("expected telemetry event to be delivered")
	}
}
