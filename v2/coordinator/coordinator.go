package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/davidroman0O/gostage/v2/state"
	"github.com/davidroman0O/gostage/v2/types"
	"github.com/davidroman0O/gostage/v2/workerhost"
)

var (
	errAlreadyStarted = errors.New("coordinator: already started")
	errHostRequired   = errors.New("coordinator: host id and instance required")
	errHostExists     = errors.New("coordinator: host already registered")
	errHostUnknown    = errors.New("coordinator: unknown host")
)

// New constructs a coordinator instance.
func New(cfg Config) (Coordinator, error) {
	if cfg.Manager == nil {
		return nil, fmt.Errorf("coordinator: manager is required")
	}
	if cfg.WorkflowFactory == nil {
		return nil, fmt.Errorf("coordinator: workflow factory is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = noopLogger{}
	}
	if cfg.ClaimInterval <= 0 {
		cfg.ClaimInterval = 50 * time.Millisecond
	}
	return &coordImpl{
		cfg:           cfg,
		logger:        logger,
		events:        make(chan Event, 128),
		inflight:      make(map[string]*inflightEntry),
		hosts:         make(map[HostID]*hostEntry),
		hostSnapshots: make(map[HostID]HostSnapshot),
	}, nil
}

type coordImpl struct {
	cfg    Config
	logger types.Logger

	mu            sync.RWMutex
	inflight      map[string]*inflightEntry
	stats         Snapshot
	hosts         map[HostID]*hostEntry
	hostSnapshots map[HostID]HostSnapshot

	events chan Event

	startOnce sync.Once
	stopOnce  sync.Once

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup
}

type hostEntry struct {
	host     workerhost.Host
	cancel   context.CancelFunc
	healthy  bool
	running  bool
	inflight int
	lastErr  error
}

type inflightEntry struct {
	workflowID string
	hostID     HostID
	leaseID    string
	lease      workerhost.Lease
	def        state.SubWorkflowDef
	claimedAt  time.Time
}

func (c *coordImpl) Start(ctx context.Context) error {
	var err error
	c.startOnce.Do(func() {
		if ctx == nil {
			err = fmt.Errorf("coordinator: start context required")
			return
		}
		c.ctx, c.cancel = context.WithCancel(ctx)

		c.mu.Lock()
		c.startHostListenersLocked()
		c.mu.Unlock()

		c.wg.Add(1)
		go c.runClaimLoop()
	})
	return err
}

func (c *coordImpl) Stop(ctx context.Context) error {
	var err error
	c.stopOnce.Do(func() {
		if c.cancel != nil {
			c.cancel()
		}
		c.mu.Lock()
		for id, entry := range c.hosts {
			if entry.cancel != nil {
				entry.cancel()
			}
			entry.running = false
			entry.healthy = false
			snap := c.hostSnapshots[id]
			snap.Healthy = false
			snap.LastError = errors.New("coordinator stop")
			c.hostSnapshots[id] = snap
		}
		released := c.drainInflightLocked()
		c.mu.Unlock()

		for _, inf := range released {
			_ = inf.lease.Release()
			_ = c.cfg.Manager.ReleaseWorkflow(inf.workflowID)
		}

		done := make(chan struct{})
		go func() {
			c.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-ctx.Done():
			err = ctx.Err()
		}
		close(c.events)
	})
	return err
}

func (c *coordImpl) RegisterHost(id HostID, host workerhost.Host) error {
	if id == "" || host == nil {
		return errHostRequired
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.hosts[id]; exists {
		return errHostExists
	}
	entry := &hostEntry{host: host, healthy: true}
	c.hosts[id] = entry
	c.hostSnapshots[id] = HostSnapshot{ID: id, Stats: host.Stats(), Healthy: true}

	if c.ctx != nil {
		c.startHostListenerLocked(id, entry)
	}
	return nil
}

func (c *coordImpl) UnregisterHost(id HostID) error {
	c.mu.Lock()
	entry, ok := c.hosts[id]
	if !ok {
		c.mu.Unlock()
		return errHostUnknown
	}
	delete(c.hosts, id)
	delete(c.hostSnapshots, id)
	if entry.cancel != nil {
		entry.cancel()
	}
	released := c.drainInflightByHostLocked(id)
	c.mu.Unlock()

	for _, inf := range released {
		_ = inf.lease.Release()
		_ = c.cfg.Manager.ReleaseWorkflow(inf.workflowID)
		c.emit(Event{Type: EventReleased, Workflow: inf.workflowID, HostID: id, LeaseID: inf.leaseID, Timestamp: time.Now(), Err: errors.New("host unregistered")})
	}
	return nil
}

func (c *coordImpl) ScaleHost(id HostID, delta int) error {
	c.mu.RLock()
	entry, ok := c.hosts[id]
	c.mu.RUnlock()
	if !ok {
		return errHostUnknown
	}
	return entry.host.Scale(delta)
}

func (c *coordImpl) Stats() Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stats
}

func (c *coordImpl) HostStats() map[HostID]HostSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	copy := make(map[HostID]HostSnapshot, len(c.hostSnapshots))
	for id, entry := range c.hosts {
		snapshot := HostSnapshot{
			ID:        id,
			Stats:     entry.host.Stats(),
			InFlight:  entry.inflight,
			Healthy:   entry.healthy,
			LastError: entry.lastErr,
		}
		c.hostSnapshots[id] = snapshot
		copy[id] = snapshot
	}
	return copy
}

func (c *coordImpl) Events() <-chan Event {
	return c.events
}

func (c *coordImpl) runClaimLoop() {
	defer c.wg.Done()
	for {
		if err := c.waitForReady(); err != nil {
			return
		}

		hostID, entry := c.chooseHost()
		if entry == nil {
			c.backoff()
			continue
		}

		lease, err := entry.host.Acquire(c.ctx)
		if err != nil {
			c.handleAcquireError(hostID, entry, err)
			continue
		}

		if !c.isHostActive(hostID) {
			_ = lease.Release()
			continue
		}

		queued, err := c.cfg.Manager.ClaimWorkflow(c.cfg.WorkerID, c.cfg.WorkerType, c.cfg.Filter)
		if err != nil {
			_ = lease.Release()
			c.backoff()
			continue
		}

		def := queued.Status.Definition
		wf, err := c.cfg.WorkflowFactory(def)
		if err != nil {
			c.logger.Error("coordinator: workflow factory error for %s: %v", def.ID, err)
			_ = c.cfg.Manager.ReleaseWorkflow(def.ID)
			_ = lease.Release()
			continue
		}

		job := workerhost.Job{
			ID:       def.ID,
			LeaseID:  def.ID,
			Workflow: wf,
			Metadata: map[string]interface{}{"worker": c.cfg.WorkerID, "host": string(hostID)},
		}

		entryData := &inflightEntry{
			workflowID: def.ID,
			hostID:     hostID,
			leaseID:    lease.ID(),
			lease:      lease,
			def:        def,
			claimedAt:  time.Now(),
		}
		c.addInflight(entryData)
		c.emit(Event{Type: EventClaimed, Workflow: def.ID, HostID: hostID, LeaseID: lease.ID(), Timestamp: time.Now()})

		if err := lease.Start(job); err != nil {
			c.logger.Error("coordinator: lease start failed for %s: %v", def.ID, err)
			c.removeInflight(def.ID)
			_ = c.cfg.Manager.ReleaseWorkflow(def.ID)
			if errors.Is(err, workerhost.ErrHostStopped) {
				c.handleHostClosed(hostID, err)
			}
			continue
		}
		c.markStarted(def.ID)
	}
}

func (c *coordImpl) waitForReady() error {
	for {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		default:
		}

		if c.reachedGlobalCapacity() {
			c.backoff()
			continue
		}
		if c.hasActiveHost() {
			return nil
		}
		c.backoff()
	}
}

func (c *coordImpl) backoff() {
	select {
	case <-time.After(c.cfg.ClaimInterval):
	case <-c.ctx.Done():
	}
}

func (c *coordImpl) hasActiveHost() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, entry := range c.hosts {
		if entry.healthy {
			return true
		}
	}
	return false
}

func (c *coordImpl) chooseHost() (HostID, *hostEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.hosts) == 0 {
		return "", nil
	}

	snapshots := make([]HostSnapshot, 0, len(c.hosts))
	for id, entry := range c.hosts {
		snapshot := HostSnapshot{
			ID:        id,
			Stats:     entry.host.Stats(),
			InFlight:  entry.inflight,
			Healthy:   entry.healthy,
			LastError: entry.lastErr,
		}
		c.hostSnapshots[id] = snapshot
		if entry.healthy {
			snapshots = append(snapshots, snapshot)
		}
	}
	if len(snapshots) == 0 {
		return "", nil
	}

	var chosen HostID
	if c.cfg.Selector != nil {
		chosen = c.cfg.Selector(snapshots)
	} else {
		chosen = defaultSelectHost(snapshots)
	}
	entry, ok := c.hosts[chosen]
	if !ok || !entry.healthy {
		return "", nil
	}
	return chosen, entry
}

func defaultSelectHost(hosts []HostSnapshot) HostID {
	var best HostSnapshot
	haveBest := false
	for _, h := range hosts {
		available := h.Stats.IdleSlots
		if !haveBest || available > best.Stats.IdleSlots || (available == best.Stats.IdleSlots && h.InFlight < best.InFlight) {
			best = h
			haveBest = true
		}
	}
	if !haveBest {
		return ""
	}
	return best.ID
}

func (c *coordImpl) handleAcquireError(id HostID, entry *hostEntry, err error) {
	if errors.Is(err, workerhost.ErrHostStopped) {
		c.handleHostClosed(id, err)
	}
	c.backoff()
}

func (c *coordImpl) isHostActive(id HostID) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.hosts[id]
	return ok && entry.healthy
}

func (c *coordImpl) startHostListenersLocked() {
	for id, entry := range c.hosts {
		c.startHostListenerLocked(id, entry)
	}
}

func (c *coordImpl) startHostListenerLocked(id HostID, entry *hostEntry) {
	if entry.running {
		return
	}
	ctx, cancel := context.WithCancel(c.ctx)
	entry.cancel = cancel
	entry.running = true
	entry.healthy = true
	c.wg.Add(1)
	go c.hostEventLoop(id, entry, ctx)
}

func (c *coordImpl) hostEventLoop(id HostID, entry *hostEntry, ctx context.Context) {
	defer c.wg.Done()
	events := entry.host.Events()
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-events:
			if !ok {
				c.handleHostClosed(id, errors.New("host events closed"))
				return
			}
			c.handleHostEvent(id, evt)
		}
	}
}

func (c *coordImpl) handleHostEvent(id HostID, evt workerhost.Event) {
	switch evt.Type {
	case workerhost.EventStarted:
		c.emit(Event{Type: EventStarted, Workflow: evt.JobID, HostID: id, LeaseID: evt.LeaseID, Timestamp: evt.Timestamp})
	case workerhost.EventCompleted:
		c.onCompletion(id, evt, true)
	case workerhost.EventFailed:
		c.onCompletion(id, evt, false)
	case workerhost.EventCancelled:
		c.onCancelled(id, evt)
	case workerhost.EventSlotAdded, workerhost.EventSlotRemoved:
		c.refreshHostSnapshot(id, nil)
	case workerhost.EventAssigned:
		// handled via claiming path; ignore duplicate emission
	}
}

func (c *coordImpl) handleHostClosed(id HostID, err error) {
	c.mu.Lock()
	entry, ok := c.hosts[id]
	if ok {
		entry.healthy = false
		entry.lastErr = err
	}
	released := c.drainInflightByHostLocked(id)
	snap := c.hostSnapshots[id]
	snap.Healthy = false
	snap.LastError = err
	c.hostSnapshots[id] = snap
	c.mu.Unlock()

	for _, inf := range released {
		_ = c.cfg.Manager.ReleaseWorkflow(inf.workflowID)
		_ = inf.lease.Release()
		c.emit(Event{Type: EventReleased, Workflow: inf.workflowID, HostID: id, LeaseID: inf.leaseID, Timestamp: time.Now(), Err: err})
	}
}

func (c *coordImpl) onCompletion(hostID HostID, evt workerhost.Event, success bool) {
	entry := c.removeInflight(evt.JobID)
	if success {
		c.updateStats(func(s *Snapshot) {
			s.Completed++
		})
		c.emit(Event{Type: EventSucceeded, Workflow: evt.JobID, HostID: hostID, LeaseID: evt.LeaseID, Timestamp: evt.Timestamp})
		_ = c.cfg.Manager.CompleteWorkflow(evt.JobID, &state.WorkflowResult{
			Success: true,
			Output:  evt.Result.FinalStore,
			EndedAt: time.Now(),
		})
		return
	}

	c.updateStats(func(s *Snapshot) {
		s.Failed++
	})
	action := FailureActionComplete
	if c.cfg.FailurePolicy != nil {
		action = c.cfg.FailurePolicy(FailureInfo{WorkflowID: evt.JobID, HostID: hostID, LeaseID: evt.LeaseID, Result: evt})
	}

	switch action {
	case FailureActionRelease:
		_ = c.cfg.Manager.ReleaseWorkflow(evt.JobID)
		c.updateStats(func(s *Snapshot) { s.Released++ })
		c.emit(Event{Type: EventReleased, Workflow: evt.JobID, HostID: hostID, LeaseID: evt.LeaseID, Timestamp: evt.Timestamp, Err: evt.Err})
	case FailureActionCancel:
		_ = c.cfg.Manager.CancelWorkflow(evt.JobID)
		c.emit(Event{Type: EventCancelled, Workflow: evt.JobID, HostID: hostID, LeaseID: evt.LeaseID, Timestamp: evt.Timestamp, Err: evt.Err})
	default:
		_ = c.cfg.Manager.CompleteWorkflow(evt.JobID, &state.WorkflowResult{
			Success: false,
			Error:   errorString(evt.Err),
			Output:  evt.Result.FinalStore,
			EndedAt: time.Now(),
		})
		c.emit(Event{Type: EventFailed, Workflow: evt.JobID, HostID: hostID, LeaseID: evt.LeaseID, Timestamp: evt.Timestamp, Err: evt.Err})
	}

	if entry != nil {
		_ = entry.lease.Release()
	}
}

func (c *coordImpl) onCancelled(hostID HostID, evt workerhost.Event) {
	entry := c.removeInflight(evt.JobID)
	c.updateStats(func(s *Snapshot) { s.Cancelled++ })
	_ = c.cfg.Manager.CancelWorkflow(evt.JobID)
	c.emit(Event{Type: EventCancelled, Workflow: evt.JobID, HostID: hostID, LeaseID: evt.LeaseID, Timestamp: evt.Timestamp, Err: evt.Err})
	if entry != nil {
		_ = entry.lease.Release()
	}
}

func (c *coordImpl) addInflight(entry *inflightEntry) {
	c.mu.Lock()
	c.inflight[entry.workflowID] = entry
	c.stats.Claimed++
	c.stats.InFlight = len(c.inflight)
	if hostEntry, ok := c.hosts[entry.hostID]; ok {
		hostEntry.inflight++
		c.refreshHostSnapshotLocked(entry.hostID, hostEntry, nil)
	}
	c.mu.Unlock()
}

func (c *coordImpl) markStarted(id string) {
	c.updateStats(func(s *Snapshot) { s.Started++ })
}

func (c *coordImpl) removeInflight(id string) *inflightEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.inflight[id]
	if ok {
		delete(c.inflight, id)
		c.stats.InFlight = len(c.inflight)
		if hostEntry, ok := c.hosts[entry.hostID]; ok {
			if hostEntry.inflight > 0 {
				hostEntry.inflight--
			}
			c.refreshHostSnapshotLocked(entry.hostID, hostEntry, nil)
		}
	}
	return entry
}

func (c *coordImpl) drainInflightLocked() []*inflightEntry {
	released := make([]*inflightEntry, 0, len(c.inflight))
	for _, inf := range c.inflight {
		released = append(released, inf)
		if hostEntry, ok := c.hosts[inf.hostID]; ok {
			hostEntry.inflight = 0
		}
	}
	c.inflight = make(map[string]*inflightEntry)
	c.stats.InFlight = 0
	for id, hostEntry := range c.hosts {
		c.refreshHostSnapshotLocked(id, hostEntry, nil)
	}
	return released
}

func (c *coordImpl) drainInflightByHostLocked(id HostID) []*inflightEntry {
	released := make([]*inflightEntry, 0)
	for wf, inf := range c.inflight {
		if inf.hostID == id {
			released = append(released, inf)
			delete(c.inflight, wf)
		}
	}
	c.stats.InFlight = len(c.inflight)
	if hostEntry, ok := c.hosts[id]; ok {
		hostEntry.inflight = 0
		c.refreshHostSnapshotLocked(id, hostEntry, nil)
	}
	return released
}

func (c *coordImpl) refreshHostSnapshot(id HostID, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.hosts[id]
	if !ok {
		return
	}
	c.refreshHostSnapshotLocked(id, entry, err)
}

func (c *coordImpl) refreshHostSnapshotLocked(id HostID, entry *hostEntry, err error) {
	snapshot := HostSnapshot{
		ID:        id,
		Stats:     entry.host.Stats(),
		InFlight:  entry.inflight,
		Healthy:   entry.healthy,
		LastError: entry.lastErr,
	}
	if err != nil {
		entry.lastErr = err
		snapshot.LastError = err
	}
	c.hostSnapshots[id] = snapshot
}

func (c *coordImpl) updateStats(fn func(*Snapshot)) {
	c.mu.Lock()
	fn(&c.stats)
	c.stats.InFlight = len(c.inflight)
	c.mu.Unlock()
}

func (c *coordImpl) emit(evt Event) {
	select {
	case c.events <- evt:
	default:
	}
}

func (c *coordImpl) reachedGlobalCapacity() bool {
	if c.cfg.MaxInFlight <= 0 {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.inflight) >= c.cfg.MaxInFlight
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

type noopLogger struct{}

func (noopLogger) Debug(string, ...interface{}) {}
func (noopLogger) Info(string, ...interface{})  {}
func (noopLogger) Warn(string, ...interface{})  {}
func (noopLogger) Error(string, ...interface{}) {}
