package testkit

import (
	"sync"
	"testing"

	"github.com/davidroman0O/gostage/v3"
)

// DiagnosticsCollector buffers diagnostic events emitted by a node.
type DiagnosticsCollector struct {
	events []gostage.DiagnosticEvent
	mu     sync.Mutex
	wg     sync.WaitGroup
}

// StartDiagnosticsCollector drains the provided channel until the caller
// invokes Close().
func StartDiagnosticsCollector(t *testing.T, ch <-chan gostage.DiagnosticEvent) *DiagnosticsCollector {
	t.Helper()
	c := &DiagnosticsCollector{}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for evt := range ch {
			c.mu.Lock()
			c.events = append(c.events, evt)
			c.mu.Unlock()
		}
	}()
	return c
}

// Events returns a copy of the collected diagnostic events.
func (c *DiagnosticsCollector) Events() []gostage.DiagnosticEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]gostage.DiagnosticEvent, len(c.events))
	copy(out, c.events)
	return out
}

// Close waits for the collector goroutine to finish.
func (c *DiagnosticsCollector) Close() {
	c.wg.Wait()
}
