package node

import (
	"github.com/davidroman0O/gostage/v3/diagnostics"
	"github.com/davidroman0O/gostage/v3/internal/locks"
)

// DiagnosticsHub provides a subscribe/broadcast mechanism for diagnostic events.
type DiagnosticsHub struct {
	mu locks.RWMutex

	subscribers []chan diagnostics.Event
	closed      bool
}

func NewDiagnosticsHub() *DiagnosticsHub {
	return &DiagnosticsHub{subscribers: make([]chan diagnostics.Event, 0)}
}

func (h *DiagnosticsHub) Subscribe() <-chan diagnostics.Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		ch := make(chan diagnostics.Event)
		close(ch)
		return ch
	}
	ch := make(chan diagnostics.Event, 64)
	h.subscribers = append(h.subscribers, ch)
	return ch
}

func (h *DiagnosticsHub) Write(evt diagnostics.Event) {
	h.mu.RLock()
	subs := append([]chan diagnostics.Event(nil), h.subscribers...)
	h.mu.RUnlock()
	for _, ch := range subs {
		func(target chan diagnostics.Event, event diagnostics.Event) {
			defer func() {
				recover()
			}()
			select {
			case target <- event:
			default:
			}
		}(ch, evt)
	}
}

func (h *DiagnosticsHub) Close() {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.closed = true
	subs := h.subscribers
	h.subscribers = nil
	h.mu.Unlock()

	for _, ch := range subs {
		close(ch)
	}
}
