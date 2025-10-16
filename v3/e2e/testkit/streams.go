package testkit

import (
	"context"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

// TelemetryBuffer captures telemetry events delivered through StreamTelemetry.
type TelemetryBuffer struct {
	events chan telemetry.Event
	cancel func()
}

// StartTelemetryBuffer subscribes to telemetry events and buffers them.
func StartTelemetryBuffer(ctx context.Context, t *testing.T, node *gostage.Node, capacity int) *TelemetryBuffer {
	t.Helper()
	if capacity <= 0 {
		capacity = 64
	}
	ch := make(chan telemetry.Event, capacity)
	cancel := node.StreamTelemetry(ctx, func(evt telemetry.Event) {
		select {
		case ch <- evt:
		default:
		}
	})
	return &TelemetryBuffer{events: ch, cancel: cancel}
}

// Next waits for the next telemetry event of the given kind.
func (b *TelemetryBuffer) Next(t *testing.T, kind telemetry.EventKind, timeout time.Duration) telemetry.Event {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case evt := <-b.events:
			if evt.Kind == kind {
				return evt
			}
		case <-deadline.C:
			t.Fatalf("timeout waiting for telemetry kind %s", kind)
		}
	}
}

// Close stops the subscription and drains the buffer.
func (b *TelemetryBuffer) Close() {
	if b == nil {
		return
	}
	if b.cancel != nil {
		b.cancel()
	}
	close(b.events)
	for range b.events {
	}
}

// HealthBuffer captures health events delivered through StreamHealth.
type HealthBuffer struct {
	events chan gostage.HealthEvent
	cancel func()
}

// StartHealthBuffer subscribes to health events for easier assertions.
func StartHealthBuffer(ctx context.Context, t *testing.T, node *gostage.Node, capacity int) *HealthBuffer {
	t.Helper()
	if capacity <= 0 {
		capacity = 32
	}
	ch := make(chan gostage.HealthEvent, capacity)
	cancel := node.StreamHealth(ctx, func(evt gostage.HealthEvent) {
		select {
		case ch <- evt:
		default:
		}
	})
	return &HealthBuffer{events: ch, cancel: cancel}
}

// Next waits for a health event with the desired status.
func (b *HealthBuffer) Next(t *testing.T, status string, timeout time.Duration) gostage.HealthEvent {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case evt := <-b.events:
			if string(evt.Status) == status {
				return evt
			}
		case <-timer.C:
			t.Fatalf("timeout waiting for health status %s", status)
		}
	}
}

// Close stops the subscription and drains events.
func (b *HealthBuffer) Close() {
	if b == nil {
		return
	}
	if b.cancel != nil {
		b.cancel()
	}
	close(b.events)
	for range b.events {
	}
}

// WaitForTelemetry consumes events from the provided channel until the desired kind arrives.
func WaitForTelemetry(t *testing.T, ch <-chan telemetry.Event, kind telemetry.EventKind, timeout time.Duration) telemetry.Event {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case evt := <-ch:
			if evt.Kind == kind {
				return evt
			}
		case <-deadline.C:
			t.Fatalf("timeout waiting for telemetry kind %s", kind)
		}
	}
}

// WaitForHealth waits for a health event matching the supplied status string.
func WaitForHealth(t *testing.T, ch <-chan gostage.HealthEvent, status string, timeout time.Duration) gostage.HealthEvent {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case evt := <-ch:
			if string(evt.Status) == status {
				return evt
			}
		case <-deadline.C:
			t.Fatalf("timeout waiting for health status %s", status)
		}
	}
}
