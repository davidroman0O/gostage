package gostage

import (
	"context"
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

	stateReader state.StateReader

	pools  []*poolBinding
	logger telemetry.Logger

	closeOnce sync.Once
	closed    atomic.Bool
}

func (n *parentNode) Submit(ctx context.Context, ref WorkflowReference, opts ...SubmitOption) (state.WorkflowID, error) {
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

	return n.queue.Enqueue(ctx, def.Clone(), cfg.priority, metadata)
}

func (n *parentNode) Wait(ctx context.Context, id state.WorkflowID) (Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	summary, err := n.store.WaitResult(ctx, id)
	if err != nil {
		return Result{}, err
	}
	return resultFromSummary(id, summary), nil
}

func (n *parentNode) Cancel(ctx context.Context, id state.WorkflowID) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return n.queue.Cancel(ctx, id)
}

func (n *parentNode) Stats(ctx context.Context) (Snapshot, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	stats, err := n.queue.Stats(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	snapshot := Snapshot{
		UpdatedAt:  time.Now(),
		QueueDepth: stats.Pending,
		InFlight:   int(n.dispatcher.inflight.Load()),
		Pools:      make([]PoolSnapshot, 0, len(n.pools)),
	}
	for _, binding := range n.pools {
		pool := binding.pool
		snapshot.Pools = append(snapshot.Pools, PoolSnapshot{
			Name:      pool.Name(),
			Slots:     pool.Slots(),
			Busy:      pool.Busy(),
			Available: pool.Available(),
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
	n.base.TelemetryDispatcher().Register(sink)
	return func() { canceled.Store(true) }
}

func (n *parentNode) StreamHealth(ctx context.Context, fn HealthHandler) CancelFunc {
	if fn == nil {
		return func() {}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var canceled atomic.Bool
	n.dispatcher.health.Subscribe(func(evt HealthEvent) {
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
	return func() { canceled.Store(true) }
}

func (n *parentNode) State() state.StateReader {
	return n.stateReader
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
	})
	return closeErr
}

func resultFromSummary(id state.WorkflowID, summary state.ResultSummary) Result {
	res := Result{
		WorkflowID:      id,
		Success:         summary.Success,
		Duration:        summary.Duration,
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
