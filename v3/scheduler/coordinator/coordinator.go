package coordinator

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/davidroman0O/gostage/v3/child"
	"github.com/davidroman0O/gostage/v3/diagnostics"
	"github.com/davidroman0O/gostage/v3/internal/locks"
	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/pools"
	"github.com/davidroman0O/gostage/v3/process"
	processproto "github.com/davidroman0O/gostage/v3/process/proto"
	"github.com/davidroman0O/gostage/v3/scheduler"
	"github.com/davidroman0O/gostage/v3/spawner"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/davidroman0O/gostage/v3/workflow"
)

const (
	remoteHeartbeatInterval = 15 * time.Second
	remoteHeartbeatTimeout  = 45 * time.Second
)

type RemoteCoordinator struct {
	ctx    context.Context
	cancel context.CancelFunc

	dispatcher *scheduler.Dispatcher
	queue      state.Queue

	telemetry   *node.TelemetryDispatcher
	diagnostics node.DiagnosticsWriter
	health      *node.HealthDispatcher
	logger      telemetry.Logger
	clock       func() time.Time

	server      *grpc.Server
	listener    net.Listener
	bindAddress string
	address     string
	tlsConfig   *tls.Config

	mu    locks.Mutex
	pools map[string]*RemotePool
	jobs  map[string]*RemoteJob

	spawned map[string]*spawner.ProcessHandle
	spawnMu locks.Mutex

	wg locks.WaitGroup
}

type RemotePool struct {
	Binding *Binding
	Worker  *RemoteWorker
}

type RemoteWorker struct {
	conn     *process.Connection
	busy     bool
	metadata map[string]string
}

type RemoteJob struct {
	Workflow *state.ClaimedWorkflow
	Pool     *RemotePool
	Release  func()
}

type processSupervisor struct {
	rc       *RemoteCoordinator
	spawner  *SpawnerBinding
	mu       locks.Mutex
	handle   *spawner.ProcessHandle
	restarts int
}

func NewRemoteCoordinator(
	ctx context.Context,
	dispatcher *scheduler.Dispatcher,
	queue state.Queue,
	telemetryDisp *node.TelemetryDispatcher,
	diag node.DiagnosticsWriter,
	health *node.HealthDispatcher,
	logger telemetry.Logger,
	bindings []*Binding,
	clock func() time.Time,
	bridgeCfg RemoteBridgeConfig,
) (*RemoteCoordinator, error) {
	hasRemote := false
	for _, binding := range bindings {
		if binding.Remote != nil {
			hasRemote = true
			break
		}
	}
	if !hasRemote {
		return nil, nil
	}
	if dispatcher == nil {
		return nil, errors.New("gostage: dispatcher required for remote coordinator")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	for _, binding := range bindings {
		if binding == nil || binding.Remote == nil {
			continue
		}
		if err := validateRemoteBinding(binding.Remote); err != nil {
			if diag != nil {
				reportRemoteValidationFailure(diag, binding, err)
			}
			bindingName := ""
			if binding.Pool != nil {
				bindingName = binding.Pool.Name()
			}
			return nil, fmt.Errorf("gostage: remote binding %q invalid: %w", bindingName, err)
		}
	}

	rcCtx, cancel := context.WithCancel(ctx)
	rc := &RemoteCoordinator{
		ctx:         rcCtx,
		cancel:      cancel,
		dispatcher:  dispatcher,
		queue:       queue,
		telemetry:   telemetryDisp,
		diagnostics: diag,
		health:      health,
		logger:      logger,
		pools:       make(map[string]*RemotePool),
		jobs:        make(map[string]*RemoteJob),
		spawned:     make(map[string]*spawner.ProcessHandle),
	}
	if clock == nil {
		rc.clock = time.Now
	} else {
		rc.clock = clock
	}

	resolvedCfg := deriveRemoteBridgeDefaults(bridgeCfg, bindings)
	bindAddr := resolvedCfg.BindAddress
	if strings.TrimSpace(bindAddr) == "" {
		bindAddr = "127.0.0.1:0"
	}

	lis, err := net.Listen("tcp", bindAddr)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("gostage: listen for remote bridge: %w", err)
	}
	rc.listener = lis
	rc.bindAddress = lis.Addr().String()
	rc.address = rc.bindAddress
	if resolvedCfg.AdvertiseAddress != "" {
		advertise := resolvedCfg.AdvertiseAddress
		if !strings.Contains(advertise, ":") {
			_, port, err := net.SplitHostPort(rc.bindAddress)
			if err == nil && port != "" {
				advertise = net.JoinHostPort(advertise, port)
			}
		}
		rc.address = advertise
	}

	serverOpts, tlsCfg, err := buildBridgeServerOptions(resolvedCfg)
	if err != nil {
		_ = lis.Close()
		cancel()
		return nil, err
	}
	rc.tlsConfig = tlsCfg

	rc.server = grpc.NewServer(serverOpts...)
	processproto.RegisterProcessBridgeServer(rc.server, process.NewServer(rc))

	rc.wg.Add(1)
	go func() {
		defer rc.wg.Done()
		if err := rc.server.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			rc.reportDiagnostic(diagnostics.Event{
				Component: "remote.bridge",
				Severity:  diagnostics.SeverityError,
				Err:       err,
			})
		}
	}()

	for _, binding := range bindings {
		if binding.Remote == nil {
			continue
		}
		rp := &RemotePool{Binding: binding}
		rc.pools[binding.Pool.Name()] = rp
		binding.Remote.Pool = rp
		binding.Remote.Coordinator = rc
		if binding.Remote.Spawner != nil && binding.Remote.Spawner.Process != nil {
			binding.Remote.Spawner.Process.SetReporter(spawner.LoggerReporter(func(evt diagnostics.Event) {
				rc.forwardChildLog(binding.Pool.Name(), evt)
			}))
		}
	}

	if err := rc.launchSpawners(); err != nil {
		rc.shutdown()
		return nil, err
	}

	return rc, nil
}

func (rc *RemoteCoordinator) launchSpawners() error {
	seen := make(map[string]struct{})
	for _, pool := range rc.pools {
		binding := pool.Binding
		if binding == nil || binding.Remote == nil || binding.Remote.Spawner == nil {
			continue
		}
		sp := binding.Remote.Spawner
		if _, ok := seen[sp.Name]; ok {
			continue
		}
		if err := rc.launchSpawner(binding); err != nil {
			return err
		}
		seen[sp.Name] = struct{}{}
	}
	return nil
}

func (rc *RemoteCoordinator) launchSpawner(binding *Binding) error {
	sp := binding.Remote.Spawner
	if sp == nil {
		return fmt.Errorf("gostage: pool %s missing spawner", binding.Pool.Name())
	}

	rc.spawnMu.Lock()
	sup := sp.Supervisor
	if sup == nil {
		sup = &processSupervisor{rc: rc, spawner: sp}
		sp.Supervisor = sup
	}
	rc.spawnMu.Unlock()

	poolCfg := binding.Remote.PoolCfg

	childType := sp.Cfg.ChildType
	if childType == "" {
		childType = poolCfg.Name
	}

	poolSpecs := rc.poolSpecsForSpawner(sp)
	if len(poolSpecs) == 0 {
		poolSpecs = []child.PoolSpec{{
			Name:     poolCfg.Name,
			Slots:    uint32(poolCfg.Slots),
			Tags:     append([]string(nil), poolCfg.Tags...),
			Metadata: PoolMetadataToStrings(poolCfg.Metadata),
		}}
	}

	launch := spawner.LaunchConfig{
		Address:   rc.address,
		AuthToken: sp.Cfg.AuthToken,
		ChildType: childType,
		Metadata:  copyStringMap(sp.Cfg.Metadata),
		TLS: child.TLSConfig{
			CertPath: sp.Cfg.TLS.CertPath,
			KeyPath:  sp.Cfg.TLS.KeyPath,
			CAPath:   sp.Cfg.TLS.CAPath,
		},
		Pools: poolSpecs,
	}
	if len(sp.Cfg.Tags) > 0 {
		if launch.Metadata == nil {
			launch.Metadata = make(map[string]string, 1)
		}
		launch.Metadata["tags"] = strings.Join(sp.Cfg.Tags, ",")
	}

	return sup.ensure(rc.ctx, launch)
}

func (rc *RemoteCoordinator) dispatch(binding *Binding, claimed *state.ClaimedWorkflow, release func()) error {
	if binding == nil || claimed == nil {
		return errors.New("gostage: remote dispatch requires binding and workflow")
	}
	initialStore := scheduler.ExtractInitialStore(claimed.Metadata)
	rc.ensureWorkflowRegistered(claimed)
	rc.mu.Lock()
	pool := rc.pools[binding.Pool.Name()]
	if pool == nil {
		rc.mu.Unlock()
		return fmt.Errorf("gostage: pool %s not registered for remote execution", binding.Pool.Name())
	}
	worker := pool.Worker
	if worker == nil {
		rc.mu.Unlock()
		return fmt.Errorf("gostage: pool %s has no active child", binding.Pool.Name())
	}
	if worker.busy {
		rc.mu.Unlock()
		return fmt.Errorf("gostage: pool %s worker busy", binding.Pool.Name())
	}
	worker.busy = true
	jobKey := string(claimed.ID)
	rc.jobs[jobKey] = &RemoteJob{Workflow: claimed, Pool: pool, Release: release}
	rc.mu.Unlock()

	rc.dispatcher.BeginRemoteDispatch(claimed.ID, func() {
		_ = rc.sendCancel(pool, claimed.ID)
	})

	if err := rc.sendLease(worker, claimed, initialStore); err != nil {
		rc.failDispatch(jobKey, err)
		return err
	}
	if mgr := rc.dispatcher.Manager(); mgr != nil {
		if err := mgr.WorkflowStatus(rc.ctx, string(claimed.ID), state.WorkflowRunning); err != nil {
			rc.dispatcher.ReportError("dispatcher.remote.status", fmt.Errorf("mark workflow %s running: %w", claimed.ID, err))
		}
	}
	return nil
}

func (rc *RemoteCoordinator) sendLease(worker *RemoteWorker, claimed *state.ClaimedWorkflow, initial map[string]any) error {
	defJSON, err := workflow.ToJSON(claimed.Definition.Clone())
	if err != nil {
		return fmt.Errorf("encode workflow definition: %w", err)
	}
	var storeJSON []byte
	if len(initial) > 0 {
		storeJSON, err = json.Marshal(initial)
		if err != nil {
			return fmt.Errorf("encode initial store: %w", err)
		}
	}
	metadataJSON, err := json.Marshal(scheduler.MetadataWithoutInitialStore(claimed.Metadata))
	if err != nil {
		return fmt.Errorf("encode metadata: %w", err)
	}

	grant := &processproto.LeaseGrant{
		WorkflowId:     string(claimed.ID),
		LeaseId:        claimed.LeaseID,
		Attempt:        int32(claimed.Attempt),
		WorkflowRef:    claimed.Definition.ID,
		DefinitionJson: defJSON,
		StoreJson:      storeJSON,
		MetadataJson:   metadataJSON,
		Priority:       int32(claimed.Priority),
	}
	if err := worker.conn.SendLeaseGrant(rc.ctx, grant); err != nil {
		return fmt.Errorf("send lease grant: %w", err)
	}
	return nil
}

func (rc *RemoteCoordinator) sendCancel(pool *RemotePool, id state.WorkflowID) error {
	rc.mu.Lock()
	worker := pool.Worker
	rc.mu.Unlock()
	if worker == nil || worker.conn == nil {
		rc.reportDiagnostic(diagnostics.Event{
			Component: "remote.cancel",
			Severity:  diagnostics.SeverityWarning,
			Metadata: map[string]any{
				"workflow_id": id,
				"pool":        pool.Binding.Pool.Name(),
				"reason":      "no worker",
			},
		})
		return errors.New("gostage: no worker to cancel")
	}
	ctx, cancel := context.WithTimeout(rc.ctx, 5*time.Second)
	defer cancel()
	err := worker.conn.SendCancel(ctx, &processproto.CancelWorkflow{WorkflowId: string(id)})
	if err != nil {
		rc.reportDiagnostic(diagnostics.Event{
			Component: "remote.cancel",
			Severity:  diagnostics.SeverityError,
			Err:       err,
			Metadata: map[string]any{
				"workflow_id": id,
				"pool":        pool.Binding.Pool.Name(),
			},
		})
	} else {
		rc.reportDiagnostic(diagnostics.Event{
			Component: "remote.cancel",
			Severity:  diagnostics.SeverityInfo,
			Metadata: map[string]any{
				"workflow_id": id,
				"pool":        pool.Binding.Pool.Name(),
			},
		})
	}
	return err
}

func (rc *RemoteCoordinator) failDispatch(jobKey string, err error) {
	rc.reportDiagnostic(diagnostics.Event{
		Component: "remote.dispatch",
		Severity:  diagnostics.SeverityError,
		Err:       err,
	})

	rc.mu.Lock()
	job := rc.jobs[jobKey]
	if job != nil {
		delete(rc.jobs, jobKey)
		if job.Pool != nil && job.Pool.Worker != nil {
			job.Pool.Worker.busy = false
		}
	}
	rc.mu.Unlock()

	if job != nil {
		rc.dispatcher.AbortRemoteDispatch(job.Workflow.ID)
		if job.Release != nil {
			job.Release()
		}
		if err := rc.queue.Release(rc.ctx, job.Workflow.ID); err != nil {
			rc.dispatcher.ReportError("dispatcher.remote.release", fmt.Errorf("release workflow %s: %w", job.Workflow.ID, err))
		}
		return
	}
	rc.dispatcher.AbortRemoteDispatch(state.WorkflowID(jobKey))
}

func (rc *RemoteCoordinator) OnRegister(ctx context.Context, conn *process.Connection, req *processproto.RegisterNode) (*processproto.RegisterAck, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing register message")
	}
	advertised := make(map[string]*processproto.ChildPool, len(req.GetPools()))
	for _, cp := range req.GetPools() {
		if cp == nil {
			continue
		}
		name := strings.TrimSpace(cp.GetName())
		if name == "" {
			continue
		}
		if _, exists := advertised[name]; !exists {
			advertised[name] = cp
		}
	}
	if len(advertised) == 0 {
		return nil, status.Error(codes.InvalidArgument, "child must declare at least one pool")
	}

	var (
		matched []*RemotePool
		unknown []string
		missing []string
		sp      *SpawnerBinding
	)

	rc.mu.Lock()
	for name := range advertised {
		pool := rc.pools[name]
		if pool == nil || pool.Binding == nil || pool.Binding.Remote == nil {
			unknown = append(unknown, name)
			continue
		}
		if sp == nil {
			sp = pool.Binding.Remote.Spawner
		}
		matched = append(matched, pool)
	}
	if len(matched) == 0 {
		rc.mu.Unlock()
		return nil, status.Errorf(codes.InvalidArgument, "child must declare at least one known pool")
	}
	if sp != nil && strings.TrimSpace(sp.Cfg.AuthToken) != "" {
		provided := strings.TrimSpace(req.GetAuthToken())
		if subtle.ConstantTimeCompare([]byte(provided), []byte(sp.Cfg.AuthToken)) != 1 {
			rc.mu.Unlock()
			meta := map[string]any{
				"node_id":    req.GetNodeId(),
				"child_type": req.GetChildType(),
				"reason":     "invalid auth token",
				"spawner":    sp.Cfg.Name,
			}
			if names := matchedPoolNames(matched); len(names) > 0 {
				meta["pools"] = names
			}
			rc.reportDiagnostic(diagnostics.Event{
				Component: "remote.register",
				Severity:  diagnostics.SeverityError,
				Metadata:  meta,
			})
			return nil, status.Error(codes.PermissionDenied, "invalid auth token")
		}
	}
	for _, pool := range matched {
		pool.Worker = &RemoteWorker{
			conn:     conn,
			metadata: copyStringStringMap(conn.Info().Metadata),
		}
	}
	if sp != nil {
		for name, pool := range rc.pools {
			if pool == nil || pool.Binding == nil || pool.Binding.Remote == nil || pool.Binding.Remote.Spawner != sp {
				continue
			}
			if _, ok := advertised[name]; ok {
				continue
			}
			pool.Worker = nil
			missing = append(missing, name)
		}
	}
	rc.mu.Unlock()

	for _, pool := range matched {
		rc.dispatcher.PublishHealth(pool.Binding.Pool.Name(), node.HealthHealthy, "child registered")
	}
	childMetadata := conn.Info().Metadata
	if len(childMetadata) == 0 && len(req.GetMetadata()) > 0 {
		childMetadata = copyStringStringMap(req.GetMetadata())
	}
	if len(childMetadata) > 0 {
		rc.mu.Lock()
		for _, pool := range matched {
			if pool.Worker != nil {
				pool.Worker.metadata = copyStringStringMap(childMetadata)
			}
		}
		rc.mu.Unlock()
	}
	if sp != nil {
		expectedMeta := expectedSpawnerMetadata(sp)
		if len(expectedMeta) > 0 {
			missing := metadataMismatches(expectedMeta, childMetadata)
			if len(missing) > 0 {
				keys := make([]string, 0, len(missing))
				for k := range missing {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				rc.reportDiagnostic(diagnostics.Event{
					Component: "remote.register",
					Severity:  diagnostics.SeverityWarning,
					Metadata: map[string]any{
						"node_id":      req.GetNodeId(),
						"child_type":   req.GetChildType(),
						"missing_keys": keys,
						"expected":     expectedMeta,
						"received":     copyStringStringMap(childMetadata),
					},
				})
			}
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		for _, name := range missing {
			rc.dispatcher.PublishHealth(name, node.HealthUnavailable, "child missing registration")
		}
		meta := map[string]any{
			"node_id":       req.GetNodeId(),
			"child_type":    req.GetChildType(),
			"missing_pools": append([]string(nil), missing...),
		}
		if sp != nil {
			meta["spawner"] = sp.Cfg.Name
		}
		rc.reportDiagnostic(diagnostics.Event{
			Component: "remote.register",
			Severity:  diagnostics.SeverityWarning,
			Metadata:  meta,
		})
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		rc.reportDiagnostic(diagnostics.Event{
			Component: "remote.register",
			Severity:  diagnostics.SeverityWarning,
			Metadata:  enrichedPoolMetadata(req, unknown),
		})
	}

	return &processproto.RegisterAck{
		HeartbeatInterval: durationpb.New(remoteHeartbeatInterval),
		HeartbeatTimeout:  durationpb.New(remoteHeartbeatTimeout),
	}, nil
}

func (rc *RemoteCoordinator) poolSpecsForSpawner(sp *SpawnerBinding) []child.PoolSpec {
	if sp == nil {
		return nil
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	specs := make([]child.PoolSpec, 0)
	seen := make(map[string]struct{})
	for name, pool := range rc.pools {
		if pool == nil || pool.Binding == nil || pool.Binding.Remote == nil {
			continue
		}
		if pool.Binding.Remote.Spawner != sp {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		cfg := pool.Binding.Remote.PoolCfg
		spec := child.PoolSpec{
			Name:     cfg.Name,
			Slots:    uint32(cfg.Slots),
			Tags:     append([]string(nil), cfg.Tags...),
			Metadata: PoolMetadataToStrings(cfg.Metadata),
		}
		if spec.Name == "" {
			spec.Name = name
		}
		if spec.Slots == 0 {
			spec.Slots = uint32(pool.Binding.Pool.Slots())
		}
		specs = append(specs, spec)
		seen[name] = struct{}{}
	}
	return specs
}

func (rc *RemoteCoordinator) OnLeaseAck(ctx context.Context, conn *process.Connection, workflowID string, leaseID string, summary state.ResultSummary) error {
	jobKey := workflowID
	rc.mu.Lock()
	job := rc.jobs[jobKey]
	if job != nil {
		delete(rc.jobs, jobKey)
		if job.Pool != nil && job.Pool.Worker != nil {
			job.Pool.Worker.busy = false
		}
	}
	rc.mu.Unlock()
	if job == nil {
		return fmt.Errorf("gostage: lease ack for unknown workflow %s", jobKey)
	}

	rc.dispatcher.UnregisterCancel(job.Workflow.ID)

	var pool *pools.Local
	if job.Pool != nil && job.Pool.Binding != nil {
		pool = job.Pool.Binding.Pool
	}
	rc.dispatcher.CompleteRemote(job.Workflow, pool, summary, job.Release)
	return nil
}

func (rc *RemoteCoordinator) OnTelemetry(ctx context.Context, conn *process.Connection, evt telemetry.Event) error {
	if rc.telemetry == nil {
		return nil
	}
	rc.enrichTelemetry(&evt)
	return rc.telemetry.Dispatch(evt)
}

func (rc *RemoteCoordinator) OnDiagnostic(ctx context.Context, conn *process.Connection, evt diagnostics.Event) {
	rc.reportDiagnostic(evt)
}

func (rc *RemoteCoordinator) OnLog(ctx context.Context, conn *process.Connection, entry process.LogEntry) error {
	severity := diagnostics.SeverityInfo
	switch strings.ToLower(entry.Level) {
	case "info", "", "debug", "trace":
		severity = diagnostics.SeverityInfo
	case "warn", "warning":
		severity = diagnostics.SeverityWarning
	case "error", "err", "fatal":
		severity = diagnostics.SeverityError
	case "panic":
		severity = diagnostics.SeverityCritical
	}

	pool := entry.Pool
	if pool == "" {
		pool = rc.poolNameForConn(conn)
	}

	metadata := make(map[string]any, len(entry.Attributes)+4)
	for k, v := range entry.Attributes {
		metadata[k] = v
	}
	metadata["message"] = entry.Message
	metadata["level"] = strings.ToLower(entry.Level)
	if entry.Logger != "" {
		metadata["logger"] = entry.Logger
	}
	metadata["structured"] = true
	if entry.ChildNodeID != "" {
		metadata["child_node_id"] = entry.ChildNodeID
	}
	if pool != "" {
		metadata["pool"] = pool
	}

	component := "remote.log"
	if pool != "" {
		component = fmt.Sprintf("remote.log.%s", pool)
	}
	if entry.OccurredAt.IsZero() {
		entry.OccurredAt = rc.now()
	}

	rc.reportDiagnostic(diagnostics.Event{
		OccurredAt: entry.OccurredAt,
		Component:  component,
		Severity:   severity,
		Metadata:   metadata,
	})
	return nil
}

func (rc *RemoteCoordinator) OnHeartbeat(ctx context.Context, conn *process.Connection, ts time.Time) error {
	return nil
}

func (rc *RemoteCoordinator) OnShutdown(ctx context.Context, conn *process.Connection, reason string) {
	if conn == nil {
		return
	}
	rc.mu.Lock()
	for name, pool := range rc.pools {
		if pool != nil && pool.Worker != nil && pool.Worker.conn == conn {
			pool.Worker = nil
			rc.dispatcher.PublishHealth(name, node.HealthUnavailable, reason)
		}
	}
	rc.mu.Unlock()
}

func (rc *RemoteCoordinator) shutdown() {
	rc.cancel()
	if rc.server != nil {
		rc.server.GracefulStop()
	}
	if rc.listener != nil {
		_ = rc.listener.Close()
	}
	rc.wg.Wait()

	rc.spawnMu.Lock()
	for name, handle := range rc.spawned {
		if handle != nil {
			_ = handle.Stop()
		}
		delete(rc.spawned, name)
	}
	rc.spawnMu.Unlock()
}

// Shutdown gracefully stops the coordinator server and releases spawned workers.
func (rc *RemoteCoordinator) Shutdown() {
	rc.shutdown()
}

func (rc *RemoteCoordinator) now() time.Time {
	if rc != nil && rc.clock != nil {
		return rc.clock()
	}
	return time.Now()
}

func (rc *RemoteCoordinator) reportDiagnostic(evt diagnostics.Event) {
	if rc.diagnostics != nil {
		rc.diagnostics.Write(evt)
	}
	if rc.logger != nil && evt.Err != nil {
		rc.logger.Error(evt.Component, "error", evt.Err)
	}
}

func (rc *RemoteCoordinator) setSpawnerHandle(sp *SpawnerBinding, handle *spawner.ProcessHandle) {
	if rc == nil || sp == nil {
		return
	}
	rc.spawnMu.Lock()
	if handle == nil {
		delete(rc.spawned, sp.Name)
	} else {
		rc.spawned[sp.Name] = handle
	}
	rc.spawnMu.Unlock()
}

func (rc *RemoteCoordinator) markSpawnerPools(sp *SpawnerBinding, status node.HealthStatus, detail string) {
	if rc == nil || rc.dispatcher == nil || sp == nil {
		return
	}
	for name, pool := range rc.pools {
		if pool == nil || pool.Binding == nil || pool.Binding.Remote == nil || pool.Binding.Remote.Spawner != sp {
			continue
		}
		rc.dispatcher.PublishHealth(name, status, detail)
	}
}

func (ps *processSupervisor) ensure(ctx context.Context, launch spawner.LaunchConfig) error {
	if ps == nil {
		return errors.New("gostage: nil supervisor")
	}
	ps.mu.Lock()
	handle := ps.handle
	ps.mu.Unlock()
	if handle != nil {
		ps.rc.setSpawnerHandle(ps.spawner, handle)
		return nil
	}
	return ps.start(ctx, launch, true)
}

func (ps *processSupervisor) start(ctx context.Context, launch spawner.LaunchConfig, reset bool) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if reset {
		ps.mu.Lock()
		ps.restarts = 0
		ps.mu.Unlock()
	}
	handle, err := ps.spawner.Process.Launch(ctx, launch)
	if err != nil {
		detail := fmt.Sprintf("launch failed: %v", err)
		ps.rc.markSpawnerPools(ps.spawner, node.HealthUnavailable, detail)
		ps.rc.reportDiagnostic(diagnostics.Event{
			Component: fmt.Sprintf("remote.spawner.%s", ps.spawner.Name),
			Severity:  diagnostics.SeverityError,
			Err:       err,
			Metadata: map[string]any{
				"restart": 0,
			},
		})
		return fmt.Errorf("gostage: launch spawner %s: %w", ps.spawner.Name, err)
	}
	ps.mu.Lock()
	ps.handle = handle
	ps.mu.Unlock()
	ps.rc.setSpawnerHandle(ps.spawner, handle)
	go ps.monitor(ctx, launch, handle)
	return nil
}

func (ps *processSupervisor) monitor(ctx context.Context, launch spawner.LaunchConfig, handle *spawner.ProcessHandle) {
	err := handle.Wait(ctx)
	ps.mu.Lock()
	if ps.handle == handle {
		ps.handle = nil
	}
	ps.mu.Unlock()
	ps.rc.setSpawnerHandle(ps.spawner, nil)

	if ctx.Err() != nil {
		return
	}

	ps.rc.markSpawnerPools(ps.spawner, node.HealthUnavailable, "child exited; restarting")

	ps.mu.Lock()
	ps.restarts++
	restarts := ps.restarts
	ps.mu.Unlock()

	metadata := map[string]any{"restart": restarts}
	if err != nil {
		metadata["error"] = err.Error()
	}

	maxRestarts := ps.spawner.Cfg.MaxRestarts
	if maxRestarts > 0 && restarts > maxRestarts {
		ps.rc.reportDiagnostic(diagnostics.Event{
			Component: fmt.Sprintf("remote.spawner.%s", ps.spawner.Name),
			Severity:  diagnostics.SeverityCritical,
			Err:       err,
			Metadata:  metadata,
		})
		return
	}

	backoff := ps.spawner.Cfg.RestartBackoff
	if backoff <= 0 {
		backoff = 100 * time.Millisecond
	}

	ps.rc.reportDiagnostic(diagnostics.Event{
		Component: fmt.Sprintf("remote.spawner.%s", ps.spawner.Name),
		Severity:  diagnostics.SeverityWarning,
		Err:       err,
		Metadata:  metadata,
	})

	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}

	_ = ps.start(ctx, launch, false)
}

func (rc *RemoteCoordinator) poolNameForConn(conn *process.Connection) string {
	if conn == nil {
		return ""
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	for name, pool := range rc.pools {
		if pool != nil && pool.Worker != nil && pool.Worker.conn == conn {
			return name
		}
	}
	return ""
}

func (rc *RemoteCoordinator) forwardChildLog(poolName string, evt diagnostics.Event) {
	if rc == nil {
		return
	}
	meta := make(map[string]any, len(evt.Metadata)+2)
	for k, v := range evt.Metadata {
		meta[k] = v
	}
	meta["stream"] = evt.Component
	if poolName == "" {
		poolName = "unknown"
	}
	meta["pool"] = poolName
	meta["structured"] = false
	rc.reportDiagnostic(diagnostics.Event{
		OccurredAt: rc.now(),
		Component:  fmt.Sprintf("remote.child.%s", poolName),
		Severity:   evt.Severity,
		Err:        evt.Err,
		Metadata:   meta,
	})
}

func (rc *RemoteCoordinator) enrichTelemetry(evt *telemetry.Event) {
	if evt == nil || evt.WorkflowID == "" {
		return
	}
	rc.mu.Lock()
	job := rc.jobs[evt.WorkflowID]
	rc.mu.Unlock()
	if job == nil || job.Pool == nil || job.Pool.Binding == nil || job.Pool.Binding.Pool == nil {
		return
	}
	if evt.Metadata == nil {
		evt.Metadata = make(map[string]any, 1)
	}
	if _, exists := evt.Metadata["pool"]; !exists {
		evt.Metadata["pool"] = job.Pool.Binding.Pool.Name()
	}
}

func deriveRemoteBridgeDefaults(cfg RemoteBridgeConfig, bindings []*Binding) RemoteBridgeConfig {
	copyCfg := cfg
	if copyCfg.TLS.CertPath == "" && copyCfg.TLS.KeyPath == "" {
		for _, binding := range bindings {
			if binding == nil || binding.Remote == nil || binding.Remote.Spawner == nil {
				continue
			}
			spTLS := binding.Remote.Spawner.Cfg.TLS
			if spTLS.CertPath != "" && spTLS.KeyPath != "" {
				copyCfg.TLS = spTLS
				if spTLS.CAPath != "" && !copyCfg.RequireClientCert {
					copyCfg.RequireClientCert = true
				}
				break
			}
		}
	}
	return copyCfg
}

func validateRemoteBinding(binding *RemoteBinding) error {
	if binding == nil || binding.Spawner == nil {
		return nil
	}

	sp := binding.Spawner
	spName := strings.TrimSpace(sp.Cfg.Name)
	if spName == "" {
		spName = strings.TrimSpace(sp.Name)
	}

	poolName := ""
	if binding.Parent != nil && binding.Parent.Pool != nil {
		poolName = binding.Parent.Pool.Name()
	}

	token := strings.TrimSpace(sp.Cfg.AuthToken)
	missingToken := token == ""

	tlsFiles := sp.Cfg.TLS
	certPath := strings.TrimSpace(tlsFiles.CertPath)
	keyPath := strings.TrimSpace(tlsFiles.KeyPath)
	caPath := strings.TrimSpace(tlsFiles.CAPath)

	hasAnyTLS := certPath != "" || keyPath != "" || caPath != ""
	missingTLS := make([]string, 0, 3)
	if hasAnyTLS {
		if certPath == "" {
			missingTLS = append(missingTLS, "cert")
		}
		if keyPath == "" {
			missingTLS = append(missingTLS, "key")
		}
		if caPath == "" {
			missingTLS = append(missingTLS, "ca")
		}
	}

	if !missingToken && len(missingTLS) == 0 {
		return nil
	}

	var errs []error
	if missingToken {
		errs = append(errs, errors.Join(ErrMissingAuthToken, fmt.Errorf("spawner %q missing auth token", spName)))
	}
	if hasAnyTLS {
		if certPath == "" || keyPath == "" {
			missingParts := make([]string, 0, 2)
			if certPath == "" {
				missingParts = append(missingParts, "cert")
			}
			if keyPath == "" {
				missingParts = append(missingParts, "key")
			}
			if len(missingParts) == 0 {
				missingParts = []string{"cert", "key"}
			}
			errs = append(errs, errors.Join(ErrMissingTLSPair, fmt.Errorf("spawner %q missing TLS %s", spName, strings.Join(missingParts, "/"))))
		}
		if caPath == "" {
			errs = append(errs, errors.Join(ErrMissingTLSCA, fmt.Errorf("spawner %q missing TLS ca", spName)))
		}
	}

	if len(errs) == 0 {
		return nil
	}

	return &remoteValidationError{
		base:         errors.Join(errs...),
		spawner:      spName,
		pool:         poolName,
		missingToken: missingToken,
		missingTLS:   missingTLS,
	}
}

// ValidateRemoteBinding exposes remote binding validation for callers that need to
// preflight configurations before starting the coordinator.
func ValidateRemoteBinding(binding *RemoteBinding) error {
	return validateRemoteBinding(binding)
}

type remoteValidationError struct {
	base         error
	spawner      string
	pool         string
	missingToken bool
	missingTLS   []string
}

func (e *remoteValidationError) Error() string {
	if e == nil || e.base == nil {
		return "remote binding validation failed"
	}
	return e.base.Error()
}

func (e *remoteValidationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.base
}

func (e *remoteValidationError) metadata(fallbackPool string) map[string]any {
	meta := map[string]any{}
	if e == nil {
		return meta
	}
	if e.spawner != "" {
		meta["spawner"] = e.spawner
	}
	pool := e.pool
	if pool == "" {
		pool = fallbackPool
	}
	if pool != "" {
		meta["pool"] = pool
	}
	if len(e.missingTLS) > 0 {
		meta["missing_tls"] = append([]string(nil), e.missingTLS...)
	}
	if e.missingToken {
		meta["missing_auth_token"] = true
	}
	return meta
}

func reportRemoteValidationFailure(diag node.DiagnosticsWriter, binding *Binding, err error) {
	if diag == nil || err == nil {
		return
	}
	var failure *remoteValidationError
	if errors.As(err, &failure) {
		poolName := ""
		if binding != nil && binding.Pool != nil {
			poolName = binding.Pool.Name()
		}
		meta := failure.metadata(poolName)
		if len(meta) == 0 {
			meta = map[string]any{}
		}
		meta["reason"] = "remote_binding_validation"
		diag.Write(diagnostics.Event{
			Component: "remote.bootstrap",
			Severity:  diagnostics.SeverityError,
			Err:       failure,
			Metadata:  meta,
		})
		return
	}
	poolName := ""
	if binding != nil && binding.Pool != nil {
		poolName = binding.Pool.Name()
	}
	meta := map[string]any{"reason": "remote_binding_validation"}
	if poolName != "" {
		meta["pool"] = poolName
	}
	diag.Write(diagnostics.Event{
		Component: "remote.bootstrap",
		Severity:  diagnostics.SeverityError,
		Err:       err,
		Metadata:  meta,
	})
}

func buildBridgeServerOptions(cfg RemoteBridgeConfig) ([]grpc.ServerOption, *tls.Config, error) {
	tlsFiles := cfg.TLS
	if tlsFiles.CertPath == "" && tlsFiles.KeyPath == "" && tlsFiles.CAPath == "" {
		return nil, nil, nil
	}
	if tlsFiles.CertPath == "" || tlsFiles.KeyPath == "" {
		return nil, nil, errors.Join(ErrMissingTLSPair, fmt.Errorf("gostage: remote bridge TLS requires cert and key"))
	}
	if tlsFiles.CAPath == "" {
		return nil, nil, errors.Join(ErrMissingTLSCA, fmt.Errorf("gostage: remote bridge TLS requires CA path"))
	}
	cert, err := tls.LoadX509KeyPair(tlsFiles.CertPath, tlsFiles.KeyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("gostage: load bridge certificate: %w", err)
	}
	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	caBytes, err := os.ReadFile(tlsFiles.CAPath)
	if err != nil {
		return nil, nil, fmt.Errorf("gostage: read bridge CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caBytes) {
		return nil, nil, errors.New("gostage: bridge CA contains no certificates")
	}
	serverTLS.ClientCAs = pool
	if cfg.RequireClientCert {
		serverTLS.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return []grpc.ServerOption{grpc.Creds(credentials.NewTLS(serverTLS))}, serverTLS, nil
}

func (rc *RemoteCoordinator) ensureWorkflowRegistered(claimed *state.ClaimedWorkflow) {
	if claimed == nil {
		return
	}
	mgr := rc.dispatcher.Manager()
	if mgr == nil {
		return
	}
	record := workflowRecordFromClaimed(claimed)
	ctx := rc.ctx
	if err := mgr.WorkflowRegistered(ctx, record); err != nil {
		rc.dispatcher.ReportError("dispatcher.remote.register", fmt.Errorf("record workflow %s: %w", claimed.ID, err))
		return
	}
	for _, stage := range claimed.Definition.Stages {
		if stage.ID == "" {
			continue
		}
		stageRec := stageRecordFromDefinition(stage)
		if err := mgr.StageRegistered(ctx, string(claimed.ID), stageRec); err != nil {
			rc.dispatcher.ReportError("dispatcher.remote.stage", fmt.Errorf("record stage %s for workflow %s: %w", stage.ID, claimed.ID, err))
			continue
		}
		for _, action := range stage.Actions {
			actionRec := actionRecordFromDefinition(action)
			if actionRec.Name == "" {
				continue
			}
			if err := mgr.ActionRegistered(ctx, string(claimed.ID), stageRec.ID, actionRec); err != nil {
				rc.dispatcher.ReportError("dispatcher.remote.action", fmt.Errorf("record action %s for workflow %s: %w", actionRec.Name, claimed.ID, err))
			}
		}
	}
	if err := mgr.WorkflowStatus(ctx, string(claimed.ID), state.WorkflowClaimed); err != nil {
		rc.dispatcher.ReportError("dispatcher.remote.status", fmt.Errorf("mark workflow %s claimed: %w", claimed.ID, err))
	}
}

func workflowRecordFromClaimed(claimed *state.ClaimedWorkflow) state.WorkflowRecord {
	if claimed == nil {
		return state.WorkflowRecord{}
	}
	created := claimed.CreatedAt
	if created.IsZero() {
		created = claimed.ClaimedAt
	}
	metadata := scheduler.MetadataWithoutInitialStore(claimed.Metadata)
	defMeta := copyMap(claimed.Definition.Metadata)
	if len(defMeta) > 0 {
		if metadata == nil {
			metadata = make(map[string]any, len(defMeta))
		}
		for k, v := range defMeta {
			if _, exists := metadata[k]; !exists {
				metadata[k] = v
			}
		}
	}
	record := state.WorkflowRecord{
		ID:          claimed.ID,
		Name:        claimed.Definition.Name,
		Description: claimed.Definition.Description,
		Type:        claimed.Definition.Type,
		Tags:        append([]string(nil), claimed.Definition.Tags...),
		Metadata:    metadata,
		CreatedAt:   created,
		State:       state.WorkflowClaimed,
		Definition: state.SubWorkflowDef{
			ID:        claimed.Definition.ID,
			Name:      claimed.Definition.Name,
			Type:      claimed.Definition.Type,
			Tags:      append([]string(nil), claimed.Definition.Tags...),
			Metadata:  copyMap(claimed.Definition.Metadata),
			Payload:   copyMap(claimed.Definition.Payload),
			Priority:  claimed.Priority,
			CreatedAt: created,
		},
	}
	return record
}

func stageRecordFromDefinition(stage workflow.Stage) state.StageRecord {
	record := state.StageRecord{
		ID:          stage.ID,
		Name:        stage.Name,
		Description: stage.Description,
		Tags:        append([]string(nil), stage.Tags...),
		Status:      state.WorkflowPending,
		Actions:     make(map[string]*state.ActionRecord),
	}
	for _, action := range stage.Actions {
		actionRec := actionRecordFromDefinition(action)
		if actionRec.Name == "" {
			continue
		}
		copy := actionRec
		record.Actions[actionRec.Name] = &copy
	}
	return record
}

func actionRecordFromDefinition(action workflow.Action) state.ActionRecord {
	name := action.ID
	if name == "" {
		name = action.Ref
	}
	return state.ActionRecord{
		Name:        name,
		Ref:         action.Ref,
		Description: action.Description,
		Tags:        append([]string(nil), action.Tags...),
		Status:      state.WorkflowPending,
	}
}

func expectedSpawnerMetadata(sp *SpawnerBinding) map[string]string {
	if sp == nil {
		return nil
	}
	meta := copyStringStringMap(sp.Cfg.Metadata)
	if len(sp.Cfg.Tags) > 0 {
		if meta == nil {
			meta = make(map[string]string, 1)
		}
		meta["tags"] = strings.Join(sp.Cfg.Tags, ",")
	}
	return meta
}

func metadataMismatches(expected, actual map[string]string) map[string]string {
	if len(expected) == 0 {
		return nil
	}
	missing := make(map[string]string)
	for key, value := range expected {
		if actual == nil {
			missing[key] = value
			continue
		}
		if actual[key] != value {
			missing[key] = value
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return missing
}

func errorFromSummary(summary state.ResultSummary) error {
	if summary.Error == "" {
		return nil
	}
	return errors.New(summary.Error)
}
func matchedPoolNames(pools []*RemotePool) []string {
	if len(pools) == 0 {
		return nil
	}
	names := make([]string, 0, len(pools))
	for _, pool := range pools {
		if pool == nil || pool.Binding == nil || pool.Binding.Pool == nil {
			continue
		}
		names = append(names, pool.Binding.Pool.Name())
	}
	return names
}

func connSpawnerName(meta map[string]string) string {
	if meta == nil {
		return ""
	}
	return meta["spawner"]
}

func enrichedPoolMetadata(req *processproto.RegisterNode, pools []string) map[string]any {
	meta := map[string]any{
		"node_id":    req.GetNodeId(),
		"child_type": req.GetChildType(),
	}
	if len(pools) > 0 {
		meta["unknown_pools"] = append([]string(nil), pools...)
	}
	if spawner := connSpawnerName(req.GetMetadata()); spawner != "" {
		meta["spawner"] = spawner
	}
	return meta
}
