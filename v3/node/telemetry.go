package node

import (
	"context"
	"fmt"
	"time"

	"github.com/davidroman0O/gostage/v3/diagnostics"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/sasha-s/go-deadlock"
)

// TelemetryDispatcher fans out runtime events to registered sinks.
type TelemetryDispatcher struct {
	mu deadlock.RWMutex

	sinks  map[int64]telemetry.Sink
	nextID int64

	ctx    context.Context
	cancel context.CancelFunc
	events chan telemetry.Event

	diag DiagnosticsWriter

	wg deadlock.WaitGroup
}

// DiagnosticsWriter is the minimal interface needed to push diagnostic events.
type DiagnosticsWriter interface {
	Write(diagnostics.Event)
}

func NewTelemetryDispatcher(ctx context.Context, diag DiagnosticsWriter) *TelemetryDispatcher {
	if ctx == nil {
		ctx = context.Background()
	}
	dispatchCtx, cancel := context.WithCancel(ctx)
	d := &TelemetryDispatcher{
		ctx:    dispatchCtx,
		cancel: cancel,
		events: make(chan telemetry.Event, 256),
		diag:   diag,
		sinks:  make(map[int64]telemetry.Sink),
	}
	d.wg.Add(1)
	go d.run()
	return d
}

func (d *TelemetryDispatcher) run() {
	defer d.wg.Done()
	for {
		select {
		case <-d.ctx.Done():
			return
		case evt := <-d.events:
			d.mu.RLock()
			sinks := make([]telemetry.Sink, 0, len(d.sinks))
			for _, sink := range d.sinks {
				sinks = append(sinks, sink)
			}
			d.mu.RUnlock()
			for _, sink := range sinks {
				sink := sink
				func() {
					defer func() {
						if r := recover(); r != nil {
							d.report(diagnostics.Event{
								Component: "telemetry.sink",
								Severity:  diagnostics.SeverityError,
								Err:       fmt.Errorf("telemetry sink panic: %v", r),
							})
						}
					}()
					sink.Record(evt)
				}()
			}
		}
	}
}

// Dispatch enqueues an event for fan-out.
func (d *TelemetryDispatcher) Dispatch(evt telemetry.Event) {
	if evt.Timestamp.IsZero() {
		evt.Timestamp = time.Now()
	}
	select {
	case d.events <- evt:
	default:
		d.report(diagnostics.Event{
			Component: "telemetry.dispatcher",
			Severity:  diagnostics.SeverityWarning,
			Err:       fmt.Errorf("telemetry buffer full; dropping event"),
			Metadata:  map[string]any{"kind": string(evt.Kind)},
		})
	}
}

// Register adds a sink (idempotent for nil values).
func (d *TelemetryDispatcher) Register(sink telemetry.Sink) func() {
	if sink == nil {
		return func() {}
	}
	d.mu.Lock()
	id := d.nextID
	d.nextID++
	d.sinks[id] = sink
	d.mu.Unlock()
	return func() {
		d.mu.Lock()
		delete(d.sinks, id)
		d.mu.Unlock()
	}
}

func (d *TelemetryDispatcher) Close() {
	d.cancel()
	d.wg.Wait()
}

func (d *TelemetryDispatcher) report(evt diagnostics.Event) {
	if d.diag != nil {
		d.diag.Write(evt)
	}
}
