package testkit

import (
	"github.com/davidroman0O/gostage/v3"
	"github.com/davidroman0O/gostage/v3/state"
)

// MemoryBackends bundles in-memory store, queue, and capture observer so tests
// can configure parent nodes without repetition.
type MemoryBackends struct {
	Store    state.Store
	Queue    state.Queue
	Observer *state.CaptureObserver
}

// NewMemoryBackends returns a fresh set of in-memory components.
func NewMemoryBackends() MemoryBackends {
	return MemoryBackends{
		Store:    state.NewMemoryStore(),
		Queue:    state.NewMemoryQueue(),
		Observer: state.NewCaptureObserver(),
	}
}

// MemoryOptions converts the provided backends into gostage options.
func MemoryOptions(b MemoryBackends) []gostage.Option {
	return []gostage.Option{
		gostage.WithStore(b.Store),
		gostage.WithQueue(b.Queue),
		gostage.WithStateObserver(b.Observer),
		gostage.WithStateReader(b.Observer.Reader()),
	}
}
