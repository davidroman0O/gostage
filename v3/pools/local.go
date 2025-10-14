package pools

import (
	"context"
	"sync/atomic"

	"github.com/davidroman0O/gostage/v3/state"
)

// Local represents a pool of local worker slots handled within the same process.
type Local struct {
	name      string
	selector  state.Selector
	slots     int
	semaphore chan struct{}
	busy      int32
}

// NewLocal constructs a Local pool with the provided selector and slot capacity.
func NewLocal(name string, selector state.Selector, slots int) *Local {
	if slots <= 0 {
		slots = 1
	}
	return &Local{
		name:      name,
		selector:  selector,
		slots:     slots,
		semaphore: make(chan struct{}, slots),
	}
}

// Name returns the pool identifier.
func (p *Local) Name() string { return p.name }

// Selector returns the queue selector associated with the pool.
func (p *Local) Selector() state.Selector { return p.selector }

// Slots returns the total capacity of the pool.
func (p *Local) Slots() int { return p.slots }

// Busy returns the number of slots currently acquired.
func (p *Local) Busy() int { return int(atomic.LoadInt32(&p.busy)) }

// TryAcquire attempts to reserve a slot. It returns a release function when successful.
func (p *Local) TryAcquire(ctx context.Context) (func(), bool) {
	select {
	case p.semaphore <- struct{}{}:
		atomic.AddInt32(&p.busy, 1)
		released := int32(0)
		release := func() {
			if atomic.CompareAndSwapInt32(&released, 0, 1) {
				<-p.semaphore
				atomic.AddInt32(&p.busy, -1)
			}
		}
		return release, true
	case <-ctx.Done():
		return nil, false
	default:
		return nil, false
	}
}

// Available reports the number of free slots.
func (p *Local) Available() int { return p.slots - p.Busy() }
