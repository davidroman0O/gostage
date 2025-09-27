package workerhost

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/davidroman0O/gostage/v2/runner"
)

type addRequest struct {
	count int
	resp  chan error
}

type removeRequest struct {
	count int
	resp  chan error
}

type acquireRequest struct {
	ctx  context.Context
	resp chan leaseResult
}

type leaseResult struct {
	lease *slotLease
	err   error
}

type host struct {
	cfg Config

	jobsCh    chan Job
	idleCh    chan *slot
	addCh     chan addRequest
	removeCh  chan removeRequest
	acquireCh chan acquireRequest
	events    chan Event
	stopCh    chan struct{}
	stopped   atomic.Bool
	stopOnce  sync.Once

	wg sync.WaitGroup

	statsMu sync.RWMutex
	stats   Stats

	slotsMu sync.Mutex
	slots   map[string]*slot

	jobCounter  atomic.Uint64
	slotCounter atomic.Uint64
}

type slot struct {
	id            string
	runner        *runner.Runner
	host          *host
	jobCh         chan Job
	stopCh        chan struct{}
	stopOnce      sync.Once
	stopRequested atomic.Bool
	reserved      atomic.Bool

	cancelMu sync.Mutex
	cancel   context.CancelFunc

	done chan struct{}
}

var ErrHostStopped = errors.New("workerhost: host stopped")

func newHost(cfg Config) (*host, error) {
	h := &host{
		cfg:       cfg,
		jobsCh:    make(chan Job, cfg.InitialSlots*4),
		idleCh:    make(chan *slot, cfg.InitialSlots*4),
		addCh:     make(chan addRequest),
		removeCh:  make(chan removeRequest),
		acquireCh: make(chan acquireRequest),
		events:    make(chan Event, cfg.InitialSlots*16),
		stopCh:    make(chan struct{}),
		slots:     make(map[string]*slot),
	}

	h.stats.TotalSlots = 0
	h.stats.IdleSlots = 0
	h.stats.BusySlots = 0
	h.stats.Inflight = 0

	// create initial slots directly to avoid channel round-trips before run loop starts
	for i := 0; i < cfg.InitialSlots; i++ {
		if err := h.createSlot(); err != nil {
			return nil, err
		}
	}

	h.wg.Add(1)
	go h.run()

	return h, nil
}

func (h *host) createSlot() error {
	br, err := h.cfg.BrokerBuilder()
	if err != nil {
		return err
	}
	r := runner.New(h.cfg.Factory, h.cfg.Registry, br, h.cfg.RunnerOptions...)
	id := fmt.Sprintf("slot-%d", h.slotCounter.Add(1))
	s := newSlot(id, r, h)

	h.slotsMu.Lock()
	h.slots[id] = s
	h.slotsMu.Unlock()

	h.updateStats(func(st *Stats) {
		st.TotalSlots++
		st.IdleSlots++
	})
	h.emitEvent(EventSlotAdded, id, "", "", runner.RunResult{}, nil, nil)

	h.wg.Add(1)
	go s.run()

	h.idleCh <- s
	return nil
}

func (h *host) run() {
	defer h.wg.Done()

	var jobQueue []Job
	var idleQueue []*slot
	var removalQueue []*removeRequest
	var acquireQueue []acquireRequest

	for {
		h.sweepCancelledAcquires(&acquireQueue)

		switch {
		case len(removalQueue) > 0 && len(idleQueue) > 0:
			sl := idleQueue[0]
			idleQueue = idleQueue[1:]
			sl.requestStop()
			h.handleSlotRemovalIdle(sl)
			removalQueue[0].count--
			if removalQueue[0].count == 0 {
				removalQueue[0].resp <- nil
				removalQueue = removalQueue[1:]
			}
			continue
		case len(acquireQueue) > 0 && len(idleQueue) > 0:
			sl := idleQueue[0]
			idleQueue = idleQueue[1:]
			req := acquireQueue[0]
			acquireQueue = acquireQueue[1:]
			if h.grantLease(sl, req) {
				continue
			}
			idleQueue = append([]*slot{sl}, idleQueue...)
			continue
		case len(jobQueue) > 0 && len(idleQueue) > 0:
			sl := idleQueue[0]
			idleQueue = idleQueue[1:]
			job := jobQueue[0]
			jobQueue = jobQueue[1:]

			if err := job.Context.Err(); err != nil {
				h.emitEvent(EventCancelled, sl.id, job.ID, job.LeaseID, runner.RunResult{}, err, job.Metadata)
				idleQueue = append([]*slot{sl}, idleQueue...)
				continue
			}
			if h.dispatchJob(sl, job, false) {
				continue
			}
			// dispatch failed (host stopping); exit
			h.cancelPendingJobs(jobQueue)
			h.cleanupIdleSlots(idleQueue)
			h.failRemovalRequests(removalQueue, ErrHostStopped)
			h.failAcquireRequests(acquireQueue, ErrHostStopped)
			return
		}

		select {
		case job, ok := <-h.jobsCh:
			if !ok {
				h.cancelPendingJobs(jobQueue)
				h.cleanupIdleSlots(idleQueue)
				h.failRemovalRequests(removalQueue, context.Canceled)
				h.failAcquireRequests(acquireQueue, context.Canceled)
				return
			}
			if job.Context == nil {
				job.Context = context.Background()
			}
			if err := job.Context.Err(); err != nil {
				h.emitEvent(EventCancelled, "", job.ID, job.LeaseID, runner.RunResult{}, err, job.Metadata)
				continue
			}
			jobQueue = append(jobQueue, job)
		case sl := <-h.idleCh:
			if sl == nil {
				continue
			}
			// handle pending removals first
			if len(removalQueue) > 0 {
				sl.requestStop()
				h.handleSlotRemovalIdle(sl)
				removalQueue[0].count--
				if removalQueue[0].count == 0 {
					removalQueue[0].resp <- nil
					removalQueue = removalQueue[1:]
				}
				continue
			}
			if h.stopped.Load() {
				sl.requestStop()
				h.handleSlotRemovalIdle(sl)
				continue
			}
			idleQueue = append(idleQueue, sl)
		case req := <-h.addCh:
			err := h.addSlots(req.count)
			req.resp <- err
		case req := <-h.removeCh:
			if req.count <= 0 {
				req.resp <- nil
				continue
			}
			if err := h.validateRemoval(req.count); err != nil {
				req.resp <- err
				continue
			}
			removalQueue = append(removalQueue, &removeRequest{count: req.count, resp: req.resp})
		case req := <-h.acquireCh:
			if req.ctx != nil && req.ctx.Err() != nil {
				req.resp <- leaseResult{lease: nil, err: req.ctx.Err()}
				continue
			}
			acquireQueue = append(acquireQueue, req)
		case <-h.stopCh:
			h.cancelPendingJobs(jobQueue)
			h.cleanupIdleSlots(idleQueue)
			h.failRemovalRequests(removalQueue, ErrHostStopped)
			h.failAcquireRequests(acquireQueue, ErrHostStopped)
			return
		}
	}
}

func (h *host) dispatchJob(slot *slot, job Job, reserved bool) bool {
	h.onJobAssigned(slot, job, reserved)
	select {
	case slot.jobCh <- job:
		if reserved {
			slot.markStarted()
		}
		return true
	case <-h.stopCh:
		h.onAssignmentFailed(slot, job, reserved)
		return false
	}
}

func (h *host) addSlots(count int) error {
	var errs []error
	for i := 0; i < count; i++ {
		if err := h.createSlot(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}

func (h *host) validateRemoval(count int) error {
	h.statsMu.RLock()
	defer h.statsMu.RUnlock()
	if count > h.stats.TotalSlots {
		return fmt.Errorf("workerhost: cannot remove %d slots (only %d available)", count, h.stats.TotalSlots)
	}
	return nil
}

func (h *host) cancelPendingJobs(jobs []Job) {
	for _, job := range jobs {
		h.emitEvent(EventCancelled, "", job.ID, job.LeaseID, runner.RunResult{}, ErrHostStopped, job.Metadata)
	}
}

func (h *host) cleanupIdleSlots(slots []*slot) {
	for _, slot := range slots {
		slot.requestStop()
		h.handleSlotRemovalIdle(slot)
	}
}

func (h *host) failRemovalRequests(reqs []*removeRequest, err error) {
	for _, req := range reqs {
		req.resp <- err
	}
}

func (h *host) failAcquireRequests(reqs []acquireRequest, err error) {
	for _, req := range reqs {
		req.resp <- leaseResult{lease: nil, err: err}
	}
}

func (h *host) sweepCancelledAcquires(queue *[]acquireRequest) {
	if len(*queue) == 0 {
		return
	}
	filtered := (*queue)[:0]
	for _, req := range *queue {
		if req.ctx != nil && req.ctx.Err() != nil {
			req.resp <- leaseResult{lease: nil, err: req.ctx.Err()}
			continue
		}
		filtered = append(filtered, req)
	}
	*queue = filtered
}

func (h *host) grantLease(sl *slot, req acquireRequest) bool {
	if h.stopped.Load() {
		req.resp <- leaseResult{lease: nil, err: ErrHostStopped}
		return false
	}
	if req.ctx != nil {
		if err := req.ctx.Err(); err != nil {
			req.resp <- leaseResult{lease: nil, err: err}
			return false
		}
	}
	if !sl.reserve() {
		req.resp <- leaseResult{lease: nil, err: fmt.Errorf("workerhost: slot %s unavailable", sl.id)}
		return false
	}
	h.updateStats(func(st *Stats) {
		st.IdleSlots--
		st.BusySlots++
	})
	lease := newSlotLease(h, sl)
	select {
	case req.resp <- leaseResult{lease: lease, err: nil}:
		return true
	default:
		h.updateStats(func(st *Stats) {
			st.IdleSlots++
			if st.BusySlots > 0 {
				st.BusySlots--
			}
		})
		sl.releaseReservation()
		return false
	}
}

func (h *host) releaseLease(sl *slot) error {
	if !sl.releaseReservation() {
		return fmt.Errorf("workerhost: slot %s lease not active", sl.id)
	}
	if h.stopped.Load() {
		h.updateStats(func(st *Stats) {
			if st.BusySlots > 0 {
				st.BusySlots--
			}
		})
		sl.requestStop()
		return ErrHostStopped
	}
	h.updateStats(func(st *Stats) {
		if st.BusySlots > 0 {
			st.BusySlots--
		}
		st.IdleSlots++
	})
	select {
	case h.idleCh <- sl:
		return nil
	case <-h.stopCh:
		sl.requestStop()
		return ErrHostStopped
	}
}

const (
	leaseStateReserved uint32 = iota
	leaseStateDispatching
	leaseStateStarted
	leaseStateReleased
)

type slotLease struct {
	host  *host
	slot  *slot
	state atomic.Uint32
}

func newSlotLease(h *host, sl *slot) *slotLease {
	l := &slotLease{host: h, slot: sl}
	l.state.Store(leaseStateReserved)
	return l
}

func (l *slotLease) ID() string { return l.slot.id }

func (l *slotLease) Start(job Job) error {
	if job.Workflow == nil {
		return fmt.Errorf("workerhost: workflow is required")
	}
	if !l.state.CompareAndSwap(leaseStateReserved, leaseStateDispatching) {
		switch l.state.Load() {
		case leaseStateStarted:
			return fmt.Errorf("workerhost: lease %s already started", l.slot.id)
		case leaseStateReleased:
			return fmt.Errorf("workerhost: lease %s already released", l.slot.id)
		default:
			return fmt.Errorf("workerhost: lease %s unavailable", l.slot.id)
		}
	}
	if job.Context == nil {
		job.Context = context.Background()
	}
	if job.Metadata == nil {
		job.Metadata = make(map[string]interface{})
	}
	if job.LeaseID == "" {
		job.LeaseID = l.slot.id
	}
	if ok := l.host.dispatchJob(l.slot, job, true); !ok {
		l.state.Store(leaseStateReleased)
		return ErrHostStopped
	}
	l.state.Store(leaseStateStarted)
	return nil
}

func (l *slotLease) Release() error {
	for {
		state := l.state.Load()
		switch state {
		case leaseStateReserved:
			if l.state.CompareAndSwap(leaseStateReserved, leaseStateReleased) {
				return l.host.releaseLease(l.slot)
			}
		case leaseStateDispatching:
			return fmt.Errorf("workerhost: lease %s dispatch in progress", l.slot.id)
		case leaseStateStarted:
			return fmt.Errorf("workerhost: lease %s already started", l.slot.id)
		case leaseStateReleased:
			return fmt.Errorf("workerhost: lease %s already released", l.slot.id)
		}
	}
}

func (h *host) emitEvent(typ EventType, slotID, jobID, leaseID string, result runner.RunResult, err error, metadata map[string]interface{}) {
	evt := Event{
		Type:      typ,
		SlotID:    slotID,
		JobID:     jobID,
		LeaseID:   leaseID,
		Result:    result,
		Err:       err,
		Metadata:  metadata,
		Timestamp: time.Now(),
	}
	select {
	case h.events <- evt:
	case <-h.stopCh:
	}
}

func (h *host) updateStats(fn func(*Stats)) {
	h.statsMu.Lock()
	defer h.statsMu.Unlock()
	fn(&h.stats)
}

func (h *host) onJobAssigned(slot *slot, job Job, reserved bool) {
	h.updateStats(func(st *Stats) {
		if reserved {
			st.Inflight++
			return
		}
		st.IdleSlots--
		st.BusySlots++
		st.Inflight++
	})
	h.emitEvent(EventAssigned, slot.id, job.ID, job.LeaseID, runner.RunResult{}, nil, job.Metadata)
}

func (h *host) onAssignmentFailed(slot *slot, job Job, reserved bool) {
	h.updateStats(func(st *Stats) {
		if reserved {
			if st.Inflight > 0 {
				st.Inflight--
			}
			return
		}
		if st.BusySlots > 0 {
			st.BusySlots--
		}
		if st.Inflight > 0 {
			st.Inflight--
		}
		st.IdleSlots++
	})
	if reserved {
		slot.releaseReservation()
	}
	h.emitEvent(EventCancelled, slot.id, job.ID, job.LeaseID, runner.RunResult{}, ErrHostStopped, job.Metadata)
}

func (h *host) onJobStarted(slot *slot, job Job) {
	h.emitEvent(EventStarted, slot.id, job.ID, job.LeaseID, runner.RunResult{}, nil, job.Metadata)
}

func (h *host) onJobFinished(slot *slot, job Job, res runner.RunResult, typ EventType, err error) {
	h.updateStats(func(st *Stats) {
		st.BusySlots--
		st.Inflight--
		if !slot.stopRequested.Load() && !h.stopped.Load() {
			st.IdleSlots++
		}
	})
	h.emitEvent(typ, slot.id, job.ID, job.LeaseID, res, err, job.Metadata)
}

func (h *host) handleSlotRemovalIdle(slot *slot) {
	if !h.removeSlotEntry(slot) {
		return
	}
	slot.requestStop()
	h.updateStats(func(st *Stats) {
		if st.IdleSlots > 0 {
			st.IdleSlots--
		}
		if st.TotalSlots > 0 {
			st.TotalSlots--
		}
	})
	h.emitEvent(EventSlotRemoved, slot.id, "", "", runner.RunResult{}, nil, nil)
}

func (h *host) handleSlotRemovalBusy(slot *slot) {
	if !h.removeSlotEntry(slot) {
		return
	}
	slot.requestStop()
	h.updateStats(func(st *Stats) {
		if st.TotalSlots > 0 {
			st.TotalSlots--
		}
	})
	h.emitEvent(EventSlotRemoved, slot.id, "", "", runner.RunResult{}, nil, nil)
}

func (h *host) removeSlotEntry(slot *slot) bool {
	h.slotsMu.Lock()
	defer h.slotsMu.Unlock()
	if _, ok := h.slots[slot.id]; ok {
		delete(h.slots, slot.id)
		return true
	}
	return false
}

func (h *host) Stats() Stats {
	h.statsMu.RLock()
	defer h.statsMu.RUnlock()
	return h.stats
}

func (h *host) Acquire(ctx context.Context) (Lease, error) {
	if h.stopped.Load() {
		return nil, ErrHostStopped
	}
	if ctx == nil {
		ctx = context.Background()
	}
	req := acquireRequest{ctx: ctx, resp: make(chan leaseResult, 1)}
	select {
	case h.acquireCh <- req:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-h.stopCh:
		return nil, ErrHostStopped
	}

	var res leaseResult
	select {
	case res = <-req.resp:
	case <-ctx.Done():
		res = <-req.resp
	case <-h.stopCh:
		res = <-req.resp
	}

	if res.err != nil {
		return nil, res.err
	}
	if err := ctx.Err(); err != nil {
		_ = res.lease.Release()
		return nil, err
	}
	if h.stopped.Load() {
		_ = res.lease.Release()
		return nil, ErrHostStopped
	}
	return res.lease, nil
}

func (h *host) Submit(job Job) error {
	if h.stopped.Load() {
		return ErrHostStopped
	}
	if job.Workflow == nil {
		return errors.New("workerhost: workflow is required")
	}
	if job.ID == "" {
		job.ID = fmt.Sprintf("job-%d", h.jobCounter.Add(1))
	}
	if job.Context == nil {
		job.Context = context.Background()
	}
	select {
	case h.jobsCh <- job:
		return nil
	case <-h.stopCh:
		return ErrHostStopped
	}
}

func (h *host) Events() <-chan Event {
	return h.events
}

func (h *host) Scale(delta int) error {
	if delta == 0 {
		return nil
	}
	if h.stopped.Load() {
		return ErrHostStopped
	}

	req := addRequest{}
	if delta > 0 {
		req = addRequest{count: delta, resp: make(chan error, 1)}
		select {
		case h.addCh <- req:
			return <-req.resp
		case <-h.stopCh:
			return ErrHostStopped
		}
	}

	rem := removeRequest{count: -delta, resp: make(chan error, 1)}
	select {
	case h.removeCh <- rem:
		return <-rem.resp
	case <-h.stopCh:
		return ErrHostStopped
	}
}

func (h *host) Stop(ctx context.Context) error {
	var once bool
	h.stopOnce.Do(func() {
		once = true
		h.stopped.Store(true)
		close(h.stopCh)

		h.slotsMu.Lock()
		for _, slot := range h.slots {
			slot.requestStop()
		}
		h.slotsMu.Unlock()

		close(h.jobsCh)
	})
	if !once {
		return nil
	}

	done := make(chan struct{})
	go func() {
		h.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		close(h.events)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
