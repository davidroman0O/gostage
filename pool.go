package gostage

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// workerPool is a bounded goroutine pool with backpressure.
// When the job channel is full, Submit blocks until a worker is available.
type workerPool struct {
	jobs    chan func()
	wg      sync.WaitGroup
	closed  atomic.Bool
	size    int
}

// defaultPoolSize returns 2 * NumCPU, clamped to a minimum of 4.
func defaultPoolSize() int {
	n := 2 * runtime.NumCPU()
	if n < 4 {
		n = 4
	}
	return n
}

// newWorkerPool creates and starts a bounded goroutine pool.
// Workers pull from a buffered job channel for backpressure.
func newWorkerPool(size int) *workerPool {
	if size <= 0 {
		size = defaultPoolSize()
	}

	p := &workerPool{
		jobs: make(chan func(), size*2), // buffer = 2x workers for throughput
		size: size,
	}

	for i := 0; i < size; i++ {
		p.wg.Add(1)
		go p.worker()
	}

	return p
}

func (p *workerPool) worker() {
	defer p.wg.Done()
	for fn := range p.jobs {
		func() {
			defer func() { recover() }()
			fn()
		}()
	}
}

// Submit queues work to the pool. Blocks if the job buffer is full (backpressure).
// Returns false if the pool is closed.
func (p *workerPool) Submit(fn func()) (ok bool) {
	if p.closed.Load() {
		return false
	}
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	p.jobs <- fn
	return true
}

// Shutdown drains pending jobs and waits for in-flight work to complete.
// If timeout > 0 and workers do not finish within the deadline, returns an error.
// If timeout <= 0, blocks indefinitely until all workers finish.
func (p *workerPool) Shutdown(timeout time.Duration) error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(p.jobs)
	if timeout <= 0 {
		p.wg.Wait()
		return nil
	}
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("pool shutdown timed out after %v", timeout)
	}
}

// Stop closes the pool without waiting for in-flight work to complete.
// Pending queued jobs will still be processed but callers don't block.
func (p *workerPool) Stop() {
	if p.closed.CompareAndSwap(false, true) {
		close(p.jobs)
		// Don't wait for workers — return immediately
	}
}

// Size returns the number of worker goroutines.
func (p *workerPool) Size() int {
	return p.size
}
