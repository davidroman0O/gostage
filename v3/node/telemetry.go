package node

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/davidroman0O/gostage/v3/diagnostics"
	"github.com/davidroman0O/gostage/v3/internal/locks"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

// OverflowStrategy controls how the dispatcher reacts when the buffer is full.
type OverflowStrategy string

const (
	// OverflowStrategyDropOldest evicts the oldest queued event to make room for the new one.
	OverflowStrategyDropOldest OverflowStrategy = "drop-oldest"
	// OverflowStrategyBlock waits up to OverflowTimeout for space before dropping the new event.
	OverflowStrategyBlock OverflowStrategy = "block"
	// OverflowStrategyFailFast immediately returns an error when the buffer is full.
	OverflowStrategyFailFast OverflowStrategy = "fail-fast"
)

// TelemetryDispatcherConfig exposes runtime tuning knobs for telemetry delivery.
type TelemetryDispatcherConfig struct {
	BufferSize       int
	OverflowStrategy OverflowStrategy
	OverflowTimeout  time.Duration
	SinkBuffer       int
}

func (c TelemetryDispatcherConfig) withDefaults() TelemetryDispatcherConfig {
	if c.BufferSize <= 0 {
		c.BufferSize = 256
	}
	if c.SinkBuffer <= 0 {
		c.SinkBuffer = 32
	}
	if c.OverflowTimeout <= 0 {
		c.OverflowTimeout = 100 * time.Millisecond
	}
	if c.OverflowStrategy == "" {
		c.OverflowStrategy = OverflowStrategyDropOldest
	}
	return c
}

// TelemetryStats reports dispatcher counters for diagnostics/monitoring.
type TelemetryStats struct {
	Dispatched  int64
	Dropped     int64
	SinkDropped int64
}

type telemetryMetrics struct {
	dispatched  atomic.Int64
	dropped     atomic.Int64
	sinkDropped atomic.Int64
}

func (m *telemetryMetrics) snapshot() TelemetryStats {
	return TelemetryStats{
		Dispatched:  m.dispatched.Load(),
		Dropped:     m.dropped.Load(),
		SinkDropped: m.sinkDropped.Load(),
	}
}

type sinkWorker struct {
	id     int64
	sink   telemetry.Sink
	ch     chan telemetry.Event
	cancel context.CancelFunc
}

// TelemetryDispatcher fans out runtime events to registered sinks with configurable backpressure.
type TelemetryDispatcher struct {
	mu     locks.RWMutex
	ctx    context.Context
	cancel context.CancelFunc

	cfg TelemetryDispatcherConfig

	diag DiagnosticsWriter

	events chan telemetry.Event

	sinks  map[int64]*sinkWorker
	nextID int64

	metrics telemetryMetrics
	wg      locks.WaitGroup

	coverageMu locks.RWMutex
	coverage   map[string]map[telemetry.EventKind]int
}

// DiagnosticsWriter is the minimal interface needed to push diagnostic events.
type DiagnosticsWriter interface {
	Write(diagnostics.Event)
}

// NewTelemetryDispatcher constructs a dispatcher with the supplied configuration.
func NewTelemetryDispatcher(ctx context.Context, diag DiagnosticsWriter, cfg TelemetryDispatcherConfig) *TelemetryDispatcher {
	cfg = cfg.withDefaults()
	if ctx == nil {
		ctx = context.Background()
	}
	dctx, cancel := context.WithCancel(ctx)
	d := &TelemetryDispatcher{
		ctx:    dctx,
		cancel: cancel,
		cfg:    cfg,
		diag:   diag,
		events: make(chan telemetry.Event, cfg.BufferSize),
		sinks:  make(map[int64]*sinkWorker),
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
			d.deliver(evt)
		}
	}
}

// Dispatch enqueues an event for fan-out respecting the configured backpressure strategy.
func (d *TelemetryDispatcher) Dispatch(evt telemetry.Event) error {
	if evt.Timestamp.IsZero() {
		evt.Timestamp = time.Now()
	}

	if evt.WorkflowID != "" && evt.Kind != "" {
		d.recordCoverage(evt.WorkflowID, evt.Kind)
	}

	switch d.cfg.OverflowStrategy {
	case OverflowStrategyDropOldest:
		select {
		case d.events <- evt:
			return nil
		default:
			select {
			case old := <-d.events:
				d.metrics.dropped.Add(1)
				d.reportOverflow(old, "drop_oldest")
			default:
			}
			select {
			case d.events <- evt:
				return nil
			default:
				d.metrics.dropped.Add(1)
				d.reportOverflow(evt, "drop_new_after_drop_oldest")
				return nil
			}
		}
	case OverflowStrategyBlock:
		timer := time.NewTimer(d.cfg.OverflowTimeout)
		defer timer.Stop()
		select {
		case d.events <- evt:
			return nil
		case <-d.ctx.Done():
			return context.Canceled
		case <-timer.C:
			d.metrics.dropped.Add(1)
			d.reportOverflow(evt, "block_timeout")
			return fmt.Errorf("telemetry dispatcher buffer full (timeout)")
		}
	case OverflowStrategyFailFast:
		select {
		case d.events <- evt:
			return nil
		default:
			d.metrics.dropped.Add(1)
			err := fmt.Errorf("telemetry dispatcher buffer full")
			d.reportOverflow(evt, "fail_fast")
			return err
		}
	}
	return nil
}

func (d *TelemetryDispatcher) deliver(evt telemetry.Event) {
	d.metrics.dispatched.Add(1)

	d.mu.RLock()
	workers := make([]*sinkWorker, 0, len(d.sinks))
	for _, worker := range d.sinks {
		workers = append(workers, worker)
	}
	d.mu.RUnlock()

	for _, worker := range workers {
		select {
		case worker.ch <- evt:
		default:
			d.metrics.sinkDropped.Add(1)
			d.report(diagnostics.Event{
				Component: "telemetry.sink",
				Severity:  diagnostics.SeverityWarning,
				Metadata: map[string]any{
					"sink_id": worker.id,
					"reason":  "sink_buffer_full",
				},
			})
		}
	}
}

func (d *TelemetryDispatcher) reportOverflow(evt telemetry.Event, reason string) {
	d.report(diagnostics.Event{
		Component: "telemetry.dispatcher",
		Severity:  diagnostics.SeverityWarning,
		Metadata: map[string]any{
			"kind":   string(evt.Kind),
			"reason": reason,
			"stats":  d.metrics.snapshot(),
		},
	})
}

func (d *TelemetryDispatcher) report(evt diagnostics.Event) {
	if d.diag != nil {
		d.diag.Write(evt)
	}
}

func (d *TelemetryDispatcher) recordCoverage(workflowID string, kind telemetry.EventKind) {
	d.coverageMu.Lock()
	if d.coverage == nil {
		d.coverage = make(map[string]map[telemetry.EventKind]int)
	}
	set := d.coverage[workflowID]
	if set == nil {
		set = make(map[telemetry.EventKind]int, 4)
		d.coverage[workflowID] = set
	}
	set[kind]++
	d.coverageMu.Unlock()
}

// Coverage returns a snapshot of telemetry events observed for the workflow.
func (d *TelemetryDispatcher) Coverage(workflowID string) map[telemetry.EventKind]int {
	if workflowID == "" {
		return nil
	}
	d.coverageMu.RLock()
	defer d.coverageMu.RUnlock()
	if d.coverage == nil {
		return nil
	}
	set := d.coverage[workflowID]
	if len(set) == 0 {
		return nil
	}
	out := make(map[telemetry.EventKind]int, len(set))
	for kind, count := range set {
		out[kind] = count
	}
	return out
}

// ClearCoverage removes coverage tracking for the workflow ID.
func (d *TelemetryDispatcher) ClearCoverage(workflowID string) {
	if workflowID == "" {
		return
	}
	d.coverageMu.Lock()
	if d.coverage != nil {
		delete(d.coverage, workflowID)
	}
	d.coverageMu.Unlock()
}

// Register adds a sink (idempotent for nil values).
func (d *TelemetryDispatcher) Register(sink telemetry.Sink) func() {
	if sink == nil {
		return func() {}
	}

	d.mu.Lock()
	id := d.nextID
	d.nextID++
	ctx, cancel := context.WithCancel(d.ctx)
	worker := &sinkWorker{
		id:     id,
		sink:   sink,
		ch:     make(chan telemetry.Event, d.cfg.SinkBuffer),
		cancel: cancel,
	}
	d.sinks[id] = worker
	d.wg.Add(1)
	d.mu.Unlock()

	go worker.run(ctx, d)

	return func() {
		d.mu.Lock()
		if w, ok := d.sinks[id]; ok {
			delete(d.sinks, id)
			w.cancel()
			close(w.ch)
		}
		d.mu.Unlock()
	}
}

func (w *sinkWorker) run(ctx context.Context, dispatcher *TelemetryDispatcher) {
	defer dispatcher.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-w.ch:
			if !ok {
				return
			}
			func() {
				defer func() {
					if r := recover(); r != nil {
						dispatcher.report(diagnostics.Event{
							Component: "telemetry.sink",
							Severity:  diagnostics.SeverityError,
							Err:       fmt.Errorf("telemetry sink panic: %v", r),
						})
					}
				}()
				w.sink.Record(evt)
			}()
		}
	}
}

// Close terminates dispatcher operations and waits for workers to exit.
func (d *TelemetryDispatcher) Close() {
	d.cancel()
	d.wg.Wait()
	d.mu.Lock()
	for id, worker := range d.sinks {
		worker.cancel()
		close(worker.ch)
		delete(d.sinks, id)
	}
	d.mu.Unlock()
}

// Stats returns a snapshot of dispatcher counters.
func (d *TelemetryDispatcher) Stats() TelemetryStats {
	return d.metrics.snapshot()
}
