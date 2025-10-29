package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/davidroman0O/gostage/v3/bootstrap"
	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/scheduler"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

type Node struct {
	base       *node.Node
	dispatcher *scheduler.Dispatcher
	remote     *RemoteCoordinator

	queue      state.Queue
	queueOwned bool

	store      state.Store
	storeOwned bool

	pools  []*PoolBinding
	logger telemetry.Logger

	sqliteDB *sql.DB
	dbOwned  bool

	closeOnce sync.Once
	closed    atomic.Bool

	State state.StateReader
}

// Stats returns scheduler metrics using a background context.
func (n *Node) Stats() (Snapshot, error) {
	if n == nil {
		return Snapshot{}, fmt.Errorf("gostage: node not initialised")
	}
	return n.stats(context.Background())
}

// StatsWithContext collects scheduler metrics using the provided context.
func (n *Node) StatsWithContext(ctx context.Context) (Snapshot, error) {
	if n == nil {
		return Snapshot{}, fmt.Errorf("gostage: node not initialised")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return n.stats(ctx)
}

func (n *Node) Submit(ctx context.Context, ref WorkflowReference, opts ...SubmitOption) (WorkflowID, error) {
	if n.closed.Load() {
		return "", ErrNodeClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ref == nil {
		return "", errors.New("gostage: workflow reference is nil")
	}
	def, err := ref.resolve()
	if err != nil {
		return "", err
	}
	cfg := bootstrap.NewSubmitRequest()
	for _, opt := range opts {
		if opt != nil {
			opt.applySubmit(cfg)
		}
	}

	if len(cfg.Tags) == 0 && len(def.Tags) > 0 {
		cfg.Tags = appendUniqueStrings(nil, def.Tags...)
	}
	if len(cfg.Tags) > 0 {
		def.Tags = appendUniqueStrings(def.Tags, cfg.Tags...)
	}

	metadata := copyMap(cfg.Metadata)
	if metadata == nil {
		metadata = make(map[string]any)
	}
	for k, v := range def.Metadata {
		if _, exists := metadata[k]; !exists {
			metadata[k] = v
		}
	}
	if cfg.InitialStore != nil {
		metadata[scheduler.MetadataInitialStore] = cfg.InitialStore
	}

	if !n.hasMatchingPool(def.Tags) {
		return "", errors.Join(ErrSubmissionRejected, ErrNoMatchingPool)
	}

	id, err := n.queue.Enqueue(ctx, def.Clone(), cfg.Priority, metadata)
	if err != nil {
		return "", err
	}
	return id, nil
}

func (n *Node) Wait(ctx context.Context, id WorkflowID) (Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	summary, err := n.store.WaitResult(ctx, state.WorkflowID(id))
	if err != nil {
		return Result{}, err
	}
	return resultFromSummary(id, summary), nil
}

func (n *Node) Cancel(ctx context.Context, id WorkflowID) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if n.dispatcher != nil {
		n.dispatcher.CancelWorkflow(state.WorkflowID(id))
	}
	if n.queue == nil {
		return nil
	}
	if err := n.queue.Cancel(ctx, state.WorkflowID(id)); err != nil && !errors.Is(err, state.ErrNoPending) {
		return err
	}
	return nil
}

func (n *Node) hasMatchingPool(tags []string) bool {
	if len(n.pools) == 0 {
		return false
	}
	for _, binding := range n.pools {
		if selectorMatches(tags, binding.Pool.Selector()) {
			return true
		}
	}
	return false
}

func (n *Node) stats(ctx context.Context) (Snapshot, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	stats, err := n.queue.Stats(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	completed64, failed64, cancelled64 := n.dispatcher.StatsCounters()
	snapshot := Snapshot{
		UpdatedAt:  time.Now(),
		QueueDepth: stats.Pending,
		InFlight:   int(n.dispatcher.Inflight()),
		Completed:  int(completed64),
		Failed:     int(failed64),
		Cancelled:  int(cancelled64),
		Pools:      make([]PoolSnapshot, 0, len(n.pools)),
	}
	for _, binding := range n.pools {
		pool := binding.Pool
		status, detail, lastChange, lastErrDetail, lastErrAt := n.dispatcher.HealthInfo(pool.Name())
		var lastErr error
		switch {
		case detail != "" && status != node.HealthHealthy:
			lastErr = errors.New(detail)
		case lastErrDetail != "":
			lastErr = errors.New(lastErrDetail)
		}
		pending := 0
		if n.queue != nil {
			count, perr := n.queue.PendingCount(ctx, pool.Selector())
			if perr != nil {
				return Snapshot{}, perr
			}
			pending = count
		}
		snapshot.Pools = append(snapshot.Pools, PoolSnapshot{
			Name:             pool.Name(),
			Slots:            pool.Slots(),
			Busy:             pool.Busy(),
			Available:        pool.Available(),
			Pending:          pending,
			Healthy:          status == node.HealthHealthy,
			Status:           status,
			LastError:        lastErr,
			LastErrorAt:      lastErrAt,
			LastHealthChange: lastChange,
		})
	}
	return snapshot, nil
}

func (n *Node) StreamTelemetry(ctx context.Context, fn TelemetryHandler) CancelFunc {
	if fn == nil {
		return func() {}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if n.base == nil {
		return func() {}
	}
	dispatcher := n.base.TelemetryDispatcher()
	if dispatcher == nil {
		return func() {}
	}
	var canceled atomic.Bool
	sink := telemetry.SinkFunc(func(evt telemetry.Event) {
		if canceled.Load() {
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
		fn(evt)
	})
	unregister := dispatcher.Register(sink)
	return func() {
		if canceled.CompareAndSwap(false, true) {
			unregister()
		}
	}
}

func (n *Node) StreamHealth(ctx context.Context, fn HealthHandler) CancelFunc {
	if fn == nil {
		return func() {}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	health := n.dispatcher.HealthDispatcher()
	if health == nil {
		return func() {}
	}
	var canceled atomic.Bool
	unregister := health.Subscribe(func(evt HealthEvent) {
		if canceled.Load() {
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
		fn(evt)
	})
	return func() {
		if canceled.CompareAndSwap(false, true) {
			unregister()
		}
	}
}

func (n *Node) Close() error {
	var closeErr error
	n.closeOnce.Do(func() {
		n.closed.Store(true)
		if n.dispatcher != nil {
			n.dispatcher.Stop()
		}
		if n.remote != nil {
			n.remote.Shutdown()
		}
		if n.base != nil {
			_ = n.base.Close()
		}
		if n.queueOwned && n.queue != nil {
			if err := n.queue.Close(); err != nil {
				closeErr = errors.Join(closeErr, err)
			}
		}
		if n.storeOwned && n.store != nil {
			if err := n.store.Close(); err != nil {
				closeErr = errors.Join(closeErr, err)
			}
		}
		if n.dbOwned && n.sqliteDB != nil {
			if err := n.sqliteDB.Close(); err != nil {
				closeErr = errors.Join(closeErr, err)
			}
		}
	})
	return closeErr
}

func resultFromSummary(id WorkflowID, summary state.ResultSummary) Result {
	res := Result{
		WorkflowID:      id,
		Success:         summary.Success,
		Duration:        summary.Duration,
		Attempt:         summary.Attempt,
		Output:          copyMap(summary.Output),
		DisabledStages:  copyBoolMap(summary.DisabledStages),
		DisabledActions: copyBoolMap(summary.DisabledActions),
		RemovedStages:   copyStringMap(summary.RemovedStages),
		RemovedActions:  copyStringMap(summary.RemovedActions),
		CompletedAt:     summary.CompletedAt,
	}
	if summary.Error != "" {
		res.Error = errors.New(summary.Error)
	}
	if summary.Reason != "" {
		res.Reason = TerminationReason(summary.Reason)
	} else {
		res.Reason = TerminationReasonUnknown
	}
	return res
}

func selectorMatches(tags []string, sel state.Selector) bool {
	if len(sel.All) == 0 && len(sel.Any) == 0 && len(sel.None) == 0 {
		return true
	}
	tagSet := make(map[string]struct{}, len(tags))
	for _, tag := range tags {
		tagSet[tag] = struct{}{}
	}
	for _, required := range sel.All {
		if _, ok := tagSet[required]; !ok {
			return false
		}
	}
	if len(sel.Any) > 0 {
		match := false
		for _, candidate := range sel.Any {
			if _, ok := tagSet[candidate]; ok {
				match = true
				break
			}
		}
		if !match {
			return false
		}
	}
	for _, excluded := range sel.None {
		if _, ok := tagSet[excluded]; ok {
			return false
		}
	}
	return true
}
