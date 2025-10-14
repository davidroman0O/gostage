package diagnostics

import (
	"time"

	"github.com/sasha-s/go-deadlock"
)

// Hub provides publish/subscribe for diagnostic events.
type Hub struct {
	mu     deadlock.RWMutex
	subs   []chan Event
	closed bool
}

func NewHub() *Hub {
	return &Hub{subs: make([]chan Event, 0)}
}

func (h *Hub) Subscribe(buffer int) <-chan Event {
	if buffer <= 0 {
		buffer = 64
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		ch := make(chan Event)
		close(ch)
		return ch
	}
	ch := make(chan Event, buffer)
	h.subs = append(h.subs, ch)
	return ch
}

func (h *Hub) Publish(evt Event) {
	if evt.OccurredAt.IsZero() {
		evt.OccurredAt = time.Now()
	}
	h.mu.RLock()
	subs := append([]chan Event(nil), h.subs...)
	h.mu.RUnlock()
	for _, ch := range subs {
		select {
		case ch <- evt:
		default:
		}
	}
}

func (h *Hub) Close() {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.closed = true
	subs := h.subs
	h.subs = nil
	h.mu.Unlock()
	for _, ch := range subs {
		close(ch)
	}
}
