package node

import (
	"strings"
	"time"

	"github.com/davidroman0O/gostage/v3/diagnostics"
	"github.com/davidroman0O/gostage/v3/internal/locks"
)

// HealthStatus represents pool health state.
type HealthStatus string

const (
	HealthHealthy     HealthStatus = "healthy"
	HealthDegraded    HealthStatus = "degraded"
	HealthUnavailable HealthStatus = "unavailable"
)

// HealthEvent is emitted when pool state changes.
type HealthEvent struct {
	Timestamp time.Time
	Pool      string
	Status    HealthStatus
	Detail    string
}

// HealthDispatcher manages subscriptions for health events.
type HealthDispatcher struct {
	mu   locks.RWMutex
	subs map[int64]func(HealthEvent)
	next int64
}

func NewHealthDispatcher() *HealthDispatcher {
	return &HealthDispatcher{subs: make(map[int64]func(HealthEvent))}
}

func (h *HealthDispatcher) Subscribe(fn func(HealthEvent)) func() {
	if fn == nil {
		return func() {}
	}
	h.mu.Lock()
	id := h.next
	h.next++
	h.subs[id] = fn
	h.mu.Unlock()
	return func() {
		h.mu.Lock()
		delete(h.subs, id)
		h.mu.Unlock()
	}
}

func (h *HealthDispatcher) Publish(evt HealthEvent) {
	h.mu.RLock()
	subs := make([]func(HealthEvent), 0, len(h.subs))
	for _, fn := range h.subs {
		subs = append(subs, fn)
	}
	h.mu.RUnlock()
	for _, fn := range subs {
		fn(evt)
	}
}

// DiagnosticsHealthWriter bridges health events into diagnostics stream when severity is high.
type DiagnosticsHealthWriter struct {
	diag DiagnosticsWriter
}

func NewDiagnosticsHealthWriter(diag DiagnosticsWriter) DiagnosticsHealthWriter {
	return DiagnosticsHealthWriter{diag: diag}
}

func (d DiagnosticsHealthWriter) Handle(evt HealthEvent) {
	if evt.Status != HealthDegraded && evt.Status != HealthUnavailable {
		return
	}
	if evt.Status == HealthUnavailable && isBenignShutdown(evt.Detail) {
		return
	}
	if d.diag != nil {
		d.diag.Write(diagnostics.Event{
			Component: "node.health",
			Severity:  diagnostics.SeverityWarning,
			Metadata: map[string]any{
				"pool":   evt.Pool,
				"status": evt.Status,
				"detail": evt.Detail,
			},
		})
	}
}

func isBenignShutdown(detail string) bool {
	if detail == "" {
		return false
	}
	lower := strings.ToLower(detail)
	if strings.Contains(lower, "context canceled") {
		return true
	}
	if strings.Contains(lower, "child closing") {
		return true
	}
	return false
}
