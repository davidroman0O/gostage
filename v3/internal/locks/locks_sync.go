//go:build !gostage_deadlock

package locks

import "sync"

// Default build uses the standard library sync primitives.
type (
	Mutex     = sync.Mutex
	RWMutex   = sync.RWMutex
	WaitGroup = sync.WaitGroup
	Once      = sync.Once
)
