package gostage

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

type parentNode struct {
	base       *node.Node
	dispatcher *dispatcher

	queue      state.Queue
	queueOwned bool

	store      state.Store
	storeOwned bool

	pools  []*poolBinding
	logger telemetry.Logger

	sqliteDB *sql.DB
	dbOwned  bool

	closeOnce sync.Once
	closed    atomic.Bool
}

func (n *parentNode) Submit(ctx context.Context, ref WorkflowReference, opts ...SubmitOption) (WorkflowID, error) {
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
	cfg := newSubmitConfig()
	for _, opt := range opts {
		if opt != nil {
			opt.applySubmit(&cfg)
		}
	}

	if len(cfg.tags) == 0 && len(def.Tags) > 0 {
		cfg.tags = appendUniqueStrings(nil, def.Tags...)
	}
	if len(cfg.tags) > 0 {
		def.Tags = appendUniqueStrings(def.Tags, cfg.tags...)
	}

	metadata := copyMap(cfg.metadata)
	if metadata == nil {
		metadata = make(map[string]any)
	}
	if cfg.initialStore != nil {
		metadata[metadataInitialStore] = cfg.initialStore
	}

	if !n.hasMatchingPool(def.Tags) {
		return "", errors.Join(ErrSubmissionRejected, ErrNoMatchingPool)
	}

	id, err := n.queue.Enqueue(ctx, def.Clone(), cfg.priority, metadata)
	if err != nil {
		return "", err
	}
	return id, nil
}

func (n *parentNode) Wait(ctx context.Context, id WorkflowID) (Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	summary, err := n.store.WaitResult(ctx, state.WorkflowID(id))
	if err != nil {
		return Result{}, err
	}
	return resultFromSummary(id, summary), nil
}

func (n *parentNode) Cancel(ctx context.Context, id WorkflowID) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if n.dispatcher != nil {
		n.dispatcher.cancelWorkflow(state.WorkflowID(id))
	}
	if n.queue == nil {
		return nil
	}
	if err := n.queue.Cancel(ctx, state.WorkflowID(id)); err != nil && !errors.Is(err, state.ErrNoPending) {
		return err
	}
	return nil
}

func (n *parentNode) hasMatchingPool(tags []string) bool {
	if len(n.pools) == 0 {
		return false
	}
	for _, binding := range n.pools {
		if selectorMatches(tags, binding.pool.Selector()) {
			return true
		}
	}
	return false
}

func (n *parentNode) stats(ctx context.Context) (Snapshot, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	stats, err := n.queue.Stats(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	completed, failed, cancelled := n.dispatcher.statsCounters()
	snapshot := Snapshot{
		UpdatedAt:  time.Now(),
		QueueDepth: stats.Pending,
		InFlight:   int(n.dispatcher.inflight.Load()),
		Completed:  completed,
		Failed:     failed,
		Cancelled:  cancelled,
		Pools:      make([]PoolSnapshot, 0, len(n.pools)),
	}
	for _, binding := range n.pools {
		pool := binding.pool
		status, detail, lastChange, lastErrDetail, lastErrAt := n.dispatcher.healthInfo(pool.Name())
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

func (n *parentNode) StreamTelemetry(ctx context.Context, fn TelemetryHandler) CancelFunc {
	if fn == nil {
		return func() {}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if n.dispatcher == nil || n.dispatcher.health == nil {
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
	unregister := n.base.TelemetryDispatcher().Register(sink)
	return func() {
		if canceled.CompareAndSwap(false, true) {
			unregister()
		}
	}
}

func (n *parentNode) StreamHealth(ctx context.Context, fn HealthHandler) CancelFunc {
	if fn == nil {
		return func() {}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var canceled atomic.Bool
	unregister := n.dispatcher.health.Subscribe(func(evt HealthEvent) {
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

func (n *parentNode) Close() error {
	var closeErr error
	n.closeOnce.Do(func() {
		n.closed.Store(true)
		if n.dispatcher != nil {
			n.dispatcher.stop()
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
