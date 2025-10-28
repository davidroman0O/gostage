package gostage

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
	"github.com/davidroman0O/gostage/v3/spawner"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/davidroman0O/gostage/v3/workflow"
)

const (
	remoteHeartbeatInterval = 15 * time.Second
	remoteHeartbeatTimeout  = 45 * time.Second
)

type remoteCoordinator struct {
	ctx    context.Context
	cancel context.CancelFunc

	dispatcher *dispatcher
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
	pools map[string]*remotePool
	jobs  map[string]*remoteJob

	spawned map[string]*spawner.ProcessHandle
	spawnMu locks.Mutex

	wg locks.WaitGroup
}

type remotePool struct {
	binding *poolBinding
	worker  *remoteWorker
}

type remoteWorker struct {
	conn     *process.Connection
	busy     bool
	metadata map[string]string
}

type remoteJob struct {
	workflow *state.ClaimedWorkflow
	pool     *remotePool
	release  func()
}

type processSupervisor struct {
	rc       *remoteCoordinator
	spawner  *spawnerBinding
	mu       locks.Mutex
	handle   *spawner.ProcessHandle
	restarts int
}

func newRemoteCoordinator(
	ctx context.Context,
	dispatcher *dispatcher,
	queue state.Queue,
	telemetryDisp *node.TelemetryDispatcher,
	diag node.DiagnosticsWriter,
	health *node.HealthDispatcher,
	logger telemetry.Logger,
	bindings []*poolBinding,
	clock func() time.Time,
	bridgeCfg RemoteBridgeConfig,
) (*remoteCoordinator, error) {
	hasRemote := false
	for _, binding := range bindings {
		if binding.remote != nil {
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

	rcCtx, cancel := context.WithCancel(ctx)
	rc := &remoteCoordinator{
		ctx:         rcCtx,
		cancel:      cancel,
		dispatcher:  dispatcher,
		queue:       queue,
		telemetry:   telemetryDisp,
		diagnostics: diag,
		health:      health,
		logger:      logger,
		pools:       make(map[string]*remotePool),
		jobs:        make(map[string]*remoteJob),
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
		if binding.remote == nil {
			continue
		}
		rp := &remotePool{binding: binding}
		rc.pools[binding.pool.Name()] = rp
		binding.remote.pool = rp
		binding.remote.coordinator = rc
		if binding.remote.spawner != nil && binding.remote.spawner.process != nil {
			binding.remote.spawner.process.SetReporter(spawner.LoggerReporter(func(evt diagnostics.Event) {
				rc.forwardChildLog(binding.pool.Name(), evt)
			}))
		}
	}

	if err := rc.launchSpawners(); err != nil {
		rc.shutdown()
		return nil, err
	}

	return rc, nil
}

func (rc *remoteCoordinator) launchSpawners() error {
	seen := make(map[string]struct{})
	for _, pool := range rc.pools {
		binding := pool.binding
		if binding == nil || binding.remote == nil || binding.remote.spawner == nil {
			continue
		}
		sp := binding.remote.spawner
		if _, ok := seen[sp.name]; ok {
			continue
		}
		if err := rc.launchSpawner(binding); err != nil {
			return err
		}
		seen[sp.name] = struct{}{}
	}
	return nil
}

func (rc *remoteCoordinator) launchSpawner(binding *poolBinding) error {
	sp := binding.remote.spawner
	if sp == nil {
		return fmt.Errorf("gostage: pool %s missing spawner", binding.pool.Name())
	}

	rc.spawnMu.Lock()
	sup := sp.supervisor
	if sup == nil {
		sup = &processSupervisor{rc: rc, spawner: sp}
		sp.supervisor = sup
	}
	rc.spawnMu.Unlock()

	poolCfg := binding.remote.poolCfg

	childType := sp.cfg.ChildType
	if childType == "" {
		childType = poolCfg.Name
	}

	poolSpecs := rc.poolSpecsForSpawner(sp)
	if len(poolSpecs) == 0 {
		poolSpecs = []child.PoolSpec{{
			Name:     poolCfg.Name,
			Slots:    uint32(poolCfg.Slots),
			Tags:     append([]string(nil), poolCfg.Tags...),
			Metadata: poolMetadataToStrings(poolCfg.Metadata),
		}}
	}

	launch := spawner.LaunchConfig{
		Address:   rc.address,
		AuthToken: sp.cfg.AuthToken,
		ChildType: childType,
		Metadata:  copyStringMap(sp.cfg.Metadata),
		TLS: child.TLSConfig{
			CertPath: sp.cfg.TLS.CertPath,
			KeyPath:  sp.cfg.TLS.KeyPath,
			CAPath:   sp.cfg.TLS.CAPath,
		},
		Pools: poolSpecs,
	}
	if len(sp.cfg.Tags) > 0 {
		if launch.Metadata == nil {
			launch.Metadata = make(map[string]string, 1)
		}
		launch.Metadata["tags"] = strings.Join(sp.cfg.Tags, ",")
	}

	return sup.ensure(rc.ctx, launch)
}

func (rc *remoteCoordinator) dispatch(binding *poolBinding, claimed *state.ClaimedWorkflow, release func()) error {
	if binding == nil || claimed == nil {
		return errors.New("gostage: remote dispatch requires binding and workflow")
	}
	initialStore := extractInitialStore(claimed.Metadata)
	rc.ensureWorkflowRegistered(claimed)
	rc.mu.Lock()
	pool := rc.pools[binding.pool.Name()]
	if pool == nil {
		rc.mu.Unlock()
		return fmt.Errorf("gostage: pool %s not registered for remote execution", binding.pool.Name())
	}
	worker := pool.worker
	if worker == nil {
		rc.mu.Unlock()
		return fmt.Errorf("gostage: pool %s has no active child", binding.pool.Name())
	}
	if worker.busy {
		rc.mu.Unlock()
		return fmt.Errorf("gostage: pool %s worker busy", binding.pool.Name())
	}
	worker.busy = true
	jobKey := string(claimed.ID)
	rc.jobs[jobKey] = &remoteJob{workflow: claimed, pool: pool, release: release}
	rc.mu.Unlock()

	rc.dispatcher.inflight.Add(1)
	rc.dispatcher.wg.Add(1)

	rc.dispatcher.registerCancel(claimed.ID, func() {
		_ = rc.sendCancel(pool, claimed.ID)
	})
	rc.dispatcher.suppressWorkflowTelemetry(claimed.ID,
		telemetry.EventWorkflowStarted,
		telemetry.EventWorkflowCompleted,
		telemetry.EventWorkflowFailed,
		telemetry.EventWorkflowCancelled,
		telemetry.EventWorkflowSummary,
	)

	if err := rc.sendLease(worker, claimed, initialStore); err != nil {
		rc.failDispatch(jobKey, err)
		return err
	}
	if rc.dispatcher != nil && rc.dispatcher.manager != nil {
		if err := rc.dispatcher.manager.WorkflowStatus(rc.ctx, string(claimed.ID), state.WorkflowRunning); err != nil {
			rc.dispatcher.reportError("dispatcher.remote.status", fmt.Errorf("mark workflow %s running: %w", claimed.ID, err))
		}
	}
	return nil
}

func (rc *remoteCoordinator) sendLease(worker *remoteWorker, claimed *state.ClaimedWorkflow, initial map[string]any) error {
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
	metadataJSON, err := json.Marshal(metadataWithoutInitialStore(claimed.Metadata))
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

func (rc *remoteCoordinator) sendCancel(pool *remotePool, id state.WorkflowID) error {
	rc.mu.Lock()
	worker := pool.worker
	rc.mu.Unlock()
	if worker == nil || worker.conn == nil {
		rc.reportDiagnostic(diagnostics.Event{
			Component: "remote.cancel",
			Severity:  diagnostics.SeverityWarning,
			Metadata: map[string]any{
				"workflow_id": id,
				"pool":        pool.binding.pool.Name(),
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
				"pool":        pool.binding.pool.Name(),
			},
		})
	} else {
		rc.reportDiagnostic(diagnostics.Event{
			Component: "remote.cancel",
			Severity:  diagnostics.SeverityInfo,
			Metadata: map[string]any{
				"workflow_id": id,
				"pool":        pool.binding.pool.Name(),
			},
		})
	}
	return err
}

func (rc *remoteCoordinator) failDispatch(jobKey string, err error) {
	rc.reportDiagnostic(diagnostics.Event{
		Component: "remote.dispatch",
		Severity:  diagnostics.SeverityError,
		Err:       err,
	})

	rc.mu.Lock()
	job := rc.jobs[jobKey]
	if job != nil {
		delete(rc.jobs, jobKey)
		if job.pool != nil && job.pool.worker != nil {
			job.pool.worker.busy = false
		}
	}
	rc.mu.Unlock()

	if job != nil {
		rc.dispatcher.unregisterCancel(job.workflow.ID)
		if job.release != nil {
			job.release()
		}
		if err := rc.queue.Release(rc.ctx, job.workflow.ID); err != nil {
			rc.dispatcher.reportError("dispatcher.remote.release", fmt.Errorf("release workflow %s: %w", job.workflow.ID, err))
		}
	}
	rc.dispatcher.inflight.Add(-1)
	rc.dispatcher.wg.Done()
}

func (rc *remoteCoordinator) OnRegister(ctx context.Context, conn *process.Connection, req *processproto.RegisterNode) (*processproto.RegisterAck, error) {
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
		matched []*remotePool
		unknown []string
		missing []string
		sp      *spawnerBinding
	)

	rc.mu.Lock()
	for name := range advertised {
		pool := rc.pools[name]
		if pool == nil || pool.binding == nil || pool.binding.remote == nil {
			unknown = append(unknown, name)
			continue
		}
		if sp == nil {
			sp = pool.binding.remote.spawner
		}
		matched = append(matched, pool)
	}
	if len(matched) == 0 {
		rc.mu.Unlock()
		return nil, status.Errorf(codes.InvalidArgument, "child must declare at least one known pool")
	}
	if sp != nil && strings.TrimSpace(sp.cfg.AuthToken) != "" {
		provided := strings.TrimSpace(req.GetAuthToken())
		if subtle.ConstantTimeCompare([]byte(provided), []byte(sp.cfg.AuthToken)) != 1 {
			rc.mu.Unlock()
			rc.reportDiagnostic(diagnostics.Event{
				Component: "remote.register",
				Severity:  diagnostics.SeverityError,
				Metadata: map[string]any{
					"node_id":    req.GetNodeId(),
					"child_type": req.GetChildType(),
					"reason":     "invalid auth token",
				},
			})
			return nil, status.Error(codes.PermissionDenied, "invalid auth token")
		}
	}
	for _, pool := range matched {
		pool.worker = &remoteWorker{
			conn:     conn,
			metadata: copyStringStringMap(conn.Info().Metadata),
		}
	}
	if sp != nil {
		for name, pool := range rc.pools {
			if pool == nil || pool.binding == nil || pool.binding.remote == nil || pool.binding.remote.spawner != sp {
				continue
			}
			if _, ok := advertised[name]; ok {
				continue
			}
			pool.worker = nil
			missing = append(missing, name)
		}
	}
	rc.mu.Unlock()

	for _, pool := range matched {
		rc.dispatcher.publishHealth(pool.binding.pool.Name(), node.HealthHealthy, "child registered")
	}
	childMetadata := conn.Info().Metadata
	if len(childMetadata) == 0 && len(req.GetMetadata()) > 0 {
		childMetadata = copyStringStringMap(req.GetMetadata())
	}
	if len(childMetadata) > 0 {
		rc.mu.Lock()
		for _, pool := range matched {
			if pool.worker != nil {
				pool.worker.metadata = copyStringStringMap(childMetadata)
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
			rc.dispatcher.publishHealth(name, node.HealthUnavailable, "child missing registration")
		}
		rc.reportDiagnostic(diagnostics.Event{
			Component: "remote.register",
			Severity:  diagnostics.SeverityWarning,
			Metadata: map[string]any{
				"node_id":       req.GetNodeId(),
				"child_type":    req.GetChildType(),
				"missing_pools": append([]string(nil), missing...),
			},
		})
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		rc.reportDiagnostic(diagnostics.Event{
			Component: "remote.register",
			Severity:  diagnostics.SeverityWarning,
			Metadata: map[string]any{
				"node_id":       req.GetNodeId(),
				"child_type":    req.GetChildType(),
				"unknown_pools": append([]string(nil), unknown...),
			},
		})
	}

	return &processproto.RegisterAck{
		HeartbeatInterval: durationpb.New(remoteHeartbeatInterval),
		HeartbeatTimeout:  durationpb.New(remoteHeartbeatTimeout),
	}, nil
}

func (rc *remoteCoordinator) poolSpecsForSpawner(sp *spawnerBinding) []child.PoolSpec {
	if sp == nil {
		return nil
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	specs := make([]child.PoolSpec, 0)
	seen := make(map[string]struct{})
	for name, pool := range rc.pools {
		if pool == nil || pool.binding == nil || pool.binding.remote == nil {
			continue
		}
		if pool.binding.remote.spawner != sp {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		cfg := pool.binding.remote.poolCfg
		spec := child.PoolSpec{
			Name:     cfg.Name,
			Slots:    uint32(cfg.Slots),
			Tags:     append([]string(nil), cfg.Tags...),
			Metadata: poolMetadataToStrings(cfg.Metadata),
		}
		if spec.Name == "" {
			spec.Name = name
		}
		if spec.Slots == 0 {
			spec.Slots = uint32(pool.binding.pool.Slots())
		}
		specs = append(specs, spec)
		seen[name] = struct{}{}
	}
	return specs
}

func (rc *remoteCoordinator) OnLeaseAck(ctx context.Context, conn *process.Connection, workflowID string, leaseID string, summary state.ResultSummary) error {
	jobKey := workflowID
	rc.mu.Lock()
	job := rc.jobs[jobKey]
	if job != nil {
		delete(rc.jobs, jobKey)
		if job.pool != nil && job.pool.worker != nil {
			job.pool.worker.busy = false
		}
	}
	rc.mu.Unlock()
	if job == nil {
		return fmt.Errorf("gostage: lease ack for unknown workflow %s", jobKey)
	}

	rc.dispatcher.unregisterCancel(job.workflow.ID)

	var pool *pools.Local
	if job.pool != nil && job.pool.binding != nil {
		pool = job.pool.binding.pool
	}
	rc.dispatcher.completeRemote(job.workflow, pool, summary, job.release)
	return nil
}

func (rc *remoteCoordinator) OnTelemetry(ctx context.Context, conn *process.Connection, evt telemetry.Event) error {
	if rc.telemetry == nil {
		return nil
	}
	rc.enrichTelemetry(&evt)
	return rc.telemetry.Dispatch(evt)
}

func (rc *remoteCoordinator) OnDiagnostic(ctx context.Context, conn *process.Connection, evt diagnostics.Event) {
	rc.reportDiagnostic(evt)
}

func (rc *remoteCoordinator) OnLog(ctx context.Context, conn *process.Connection, entry process.LogEntry) error {
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

func (rc *remoteCoordinator) OnHeartbeat(ctx context.Context, conn *process.Connection, ts time.Time) error {
	return nil
}

func (rc *remoteCoordinator) OnShutdown(ctx context.Context, conn *process.Connection, reason string) {
	if conn == nil {
		return
	}
	rc.mu.Lock()
	for name, pool := range rc.pools {
		if pool != nil && pool.worker != nil && pool.worker.conn == conn {
			pool.worker = nil
			rc.dispatcher.publishHealth(name, node.HealthUnavailable, reason)
		}
	}
	rc.mu.Unlock()
}

func (rc *remoteCoordinator) shutdown() {
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

func (rc *remoteCoordinator) now() time.Time {
	if rc != nil && rc.clock != nil {
		return rc.clock()
	}
	return time.Now()
}

func (rc *remoteCoordinator) reportDiagnostic(evt diagnostics.Event) {
	if rc.diagnostics != nil {
		rc.diagnostics.Write(evt)
	}
	if rc.logger != nil && evt.Err != nil {
		rc.logger.Error(evt.Component, "error", evt.Err)
	}
}

func (rc *remoteCoordinator) setSpawnerHandle(sp *spawnerBinding, handle *spawner.ProcessHandle) {
	if rc == nil || sp == nil {
		return
	}
	rc.spawnMu.Lock()
	if handle == nil {
		delete(rc.spawned, sp.name)
	} else {
		rc.spawned[sp.name] = handle
	}
	rc.spawnMu.Unlock()
}

func (rc *remoteCoordinator) markSpawnerPools(sp *spawnerBinding, status node.HealthStatus, detail string) {
	if rc == nil || rc.dispatcher == nil || sp == nil {
		return
	}
	for name, pool := range rc.pools {
		if pool == nil || pool.binding == nil || pool.binding.remote == nil || pool.binding.remote.spawner != sp {
			continue
		}
		rc.dispatcher.publishHealth(name, status, detail)
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
	handle, err := ps.spawner.process.Launch(ctx, launch)
	if err != nil {
		detail := fmt.Sprintf("launch failed: %v", err)
		ps.rc.markSpawnerPools(ps.spawner, node.HealthUnavailable, detail)
		ps.rc.reportDiagnostic(diagnostics.Event{
			Component: fmt.Sprintf("remote.spawner.%s", ps.spawner.name),
			Severity:  diagnostics.SeverityError,
			Err:       err,
			Metadata: map[string]any{
				"restart": 0,
			},
		})
		return fmt.Errorf("gostage: launch spawner %s: %w", ps.spawner.name, err)
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

	maxRestarts := ps.spawner.cfg.MaxRestarts
	if maxRestarts > 0 && restarts > maxRestarts {
		ps.rc.reportDiagnostic(diagnostics.Event{
			Component: fmt.Sprintf("remote.spawner.%s", ps.spawner.name),
			Severity:  diagnostics.SeverityCritical,
			Err:       err,
			Metadata:  metadata,
		})
		return
	}

	backoff := ps.spawner.cfg.RestartBackoff
	if backoff <= 0 {
		backoff = 100 * time.Millisecond
	}

	ps.rc.reportDiagnostic(diagnostics.Event{
		Component: fmt.Sprintf("remote.spawner.%s", ps.spawner.name),
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

func (rc *remoteCoordinator) poolNameForConn(conn *process.Connection) string {
	if conn == nil {
		return ""
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	for name, pool := range rc.pools {
		if pool != nil && pool.worker != nil && pool.worker.conn == conn {
			return name
		}
	}
	return ""
}

func (rc *remoteCoordinator) forwardChildLog(poolName string, evt diagnostics.Event) {
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

func (rc *remoteCoordinator) enrichTelemetry(evt *telemetry.Event) {
	if evt == nil || evt.WorkflowID == "" {
		return
	}
	rc.mu.Lock()
	job := rc.jobs[evt.WorkflowID]
	rc.mu.Unlock()
	if job == nil || job.pool == nil || job.pool.binding == nil || job.pool.binding.pool == nil {
		return
	}
	if evt.Metadata == nil {
		evt.Metadata = make(map[string]any, 1)
	}
	if _, exists := evt.Metadata["pool"]; !exists {
		evt.Metadata["pool"] = job.pool.binding.pool.Name()
	}
}

func deriveRemoteBridgeDefaults(cfg RemoteBridgeConfig, bindings []*poolBinding) RemoteBridgeConfig {
	copyCfg := cfg
	if copyCfg.TLS.CertPath == "" && copyCfg.TLS.KeyPath == "" {
		for _, binding := range bindings {
			if binding == nil || binding.remote == nil || binding.remote.spawner == nil {
				continue
			}
			spTLS := binding.remote.spawner.cfg.TLS
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

func buildBridgeServerOptions(cfg RemoteBridgeConfig) ([]grpc.ServerOption, *tls.Config, error) {
	tlsFiles := cfg.TLS
	if tlsFiles.CertPath == "" && tlsFiles.KeyPath == "" {
		return nil, nil, nil
	}
	if tlsFiles.CertPath == "" || tlsFiles.KeyPath == "" {
		return nil, nil, fmt.Errorf("gostage: remote bridge TLS requires both cert and key")
	}
	cert, err := tls.LoadX509KeyPair(tlsFiles.CertPath, tlsFiles.KeyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("gostage: load bridge certificate: %w", err)
	}
	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if tlsFiles.CAPath != "" {
		caBytes, err := os.ReadFile(tlsFiles.CAPath)
		if err != nil {
			return nil, nil, fmt.Errorf("gostage: read bridge CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caBytes) {
			return nil, nil, errors.New("gostage: bridge CA contains no certificates")
		}
		serverTLS.ClientCAs = pool
		serverTLS.ClientAuth = tls.RequireAndVerifyClientCert
	} else if cfg.RequireClientCert {
		serverTLS.ClientAuth = tls.RequireAnyClientCert
	}
	return []grpc.ServerOption{grpc.Creds(credentials.NewTLS(serverTLS))}, serverTLS, nil
}

func (rc *remoteCoordinator) ensureWorkflowRegistered(claimed *state.ClaimedWorkflow) {
	if claimed == nil || rc.dispatcher == nil || rc.dispatcher.manager == nil {
		return
	}
	record := workflowRecordFromClaimed(claimed)
	ctx := rc.ctx
	if err := rc.dispatcher.manager.WorkflowRegistered(ctx, record); err != nil {
		rc.dispatcher.reportError("dispatcher.remote.register", fmt.Errorf("record workflow %s: %w", claimed.ID, err))
		return
	}
	for _, stage := range claimed.Definition.Stages {
		if stage.ID == "" {
			continue
		}
		stageRec := stageRecordFromDefinition(stage)
		if err := rc.dispatcher.manager.StageRegistered(ctx, string(claimed.ID), stageRec); err != nil {
			rc.dispatcher.reportError("dispatcher.remote.stage", fmt.Errorf("record stage %s for workflow %s: %w", stage.ID, claimed.ID, err))
			continue
		}
		for _, action := range stage.Actions {
			actionRec := actionRecordFromDefinition(action)
			if actionRec.Name == "" {
				continue
			}
			if err := rc.dispatcher.manager.ActionRegistered(ctx, string(claimed.ID), stageRec.ID, actionRec); err != nil {
				rc.dispatcher.reportError("dispatcher.remote.action", fmt.Errorf("record action %s for workflow %s: %w", actionRec.Name, claimed.ID, err))
			}
		}
	}
	if err := rc.dispatcher.manager.WorkflowStatus(ctx, string(claimed.ID), state.WorkflowClaimed); err != nil {
		rc.dispatcher.reportError("dispatcher.remote.status", fmt.Errorf("mark workflow %s claimed: %w", claimed.ID, err))
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
	metadata := metadataWithoutInitialStore(claimed.Metadata)
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

func expectedSpawnerMetadata(sp *spawnerBinding) map[string]string {
	if sp == nil {
		return nil
	}
	meta := copyStringStringMap(sp.cfg.Metadata)
	if len(sp.cfg.Tags) > 0 {
		if meta == nil {
			meta = make(map[string]string, 1)
		}
		meta["tags"] = strings.Join(sp.cfg.Tags, ",")
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
