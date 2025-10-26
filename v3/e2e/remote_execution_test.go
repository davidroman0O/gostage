package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3"
	"github.com/davidroman0O/gostage/v3/child"
	"github.com/davidroman0O/gostage/v3/diagnostics"
	"github.com/davidroman0O/gostage/v3/e2e/testkit"
	"github.com/davidroman0O/gostage/v3/node"
	rt "github.com/davidroman0O/gostage/v3/runtime"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/store"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/davidroman0O/gostage/v3/workflow"
)

const (
	remoteSpawnerName = "e2e-remote-spawner"
	remotePoolName    = "e2e-remote-pool"
	remoteAltPoolName = "e2e-remote-pool-alt"
	remoteChildType   = "e2e-remote-child"
	childInfoLogPrefix = "CHILD-INFO"
)

var modeFlag = flag.String("mode", "", "gostage execution mode")

var (
	childInfoMu           sync.Mutex
	lastChildType         string
	lastChildMetadata     map[string]string
	remoteSpawnerMetadata = map[string]string{"region": "test-lab"}
)

func resetChildInfo() {
	childInfoMu.Lock()
	defer childInfoMu.Unlock()
	lastChildType = ""
	lastChildMetadata = nil
}

func recordChildInfo(cfg child.Config) {
	childInfoMu.Lock()
	lastChildType = cfg.ChildType
	if len(cfg.Metadata) == 0 {
		lastChildMetadata = nil
	} else {
		meta := make(map[string]string, len(cfg.Metadata))
		for k, v := range cfg.Metadata {
			meta[k] = v
		}
		lastChildMetadata = meta
	}
	childInfoMu.Unlock()

	payload := struct {
		Type     string            `json:"type"`
		Metadata map[string]string `json:"metadata,omitempty"`
		Pools    []string          `json:"pools,omitempty"`
	}{
		Type:     cfg.ChildType,
		Metadata: cfg.Metadata,
	}
	if len(cfg.Pools) > 0 {
		names := make([]string, 0, len(cfg.Pools))
		for _, pool := range cfg.Pools {
			if pool.Name != "" {
				names = append(names, pool.Name)
			}
		}
		if len(names) > 0 {
			payload.Pools = names
		}
	}
	if data, err := json.Marshal(payload); err == nil {
		fmt.Printf("%s %s\n", childInfoLogPrefix, string(data))
	}
}

func expectChildInfo(t *testing.T, typ string, expected map[string]string) {
	t.Helper()
	childInfoMu.Lock()
	defer childInfoMu.Unlock()
	if lastChildType != typ {
		t.Fatalf("expected child type %q, got %q", typ, lastChildType)
	}
	if len(expected) != len(lastChildMetadata) {
		t.Fatalf("child metadata mismatch: expected %d entries, got %d (%v)", len(expected), len(lastChildMetadata), lastChildMetadata)
	}
	for k, v := range expected {
		if got, ok := lastChildMetadata[k]; !ok || got != v {
			t.Fatalf("child metadata mismatch for key %q: expected %q, got %q", k, v, got)
		}
	}
}

func harvestChildInfo(diag *testkit.DiagnosticsCollector) {
	if diag == nil {
		return
	}
	events := diag.Events()
	for _, evt := range events {
		line, _ := evt.Metadata["line"].(string)
		if !strings.HasPrefix(line, childInfoLogPrefix+" ") {
			continue
		}
		payload := strings.TrimPrefix(line, childInfoLogPrefix+" ")
		var info struct {
			Type     string            `json:"type"`
			Metadata map[string]string `json:"metadata"`
		}
		if err := json.Unmarshal([]byte(payload), &info); err != nil {
			continue
		}
		childInfoMu.Lock()
		lastChildType = info.Type
		if len(info.Metadata) == 0 {
			lastChildMetadata = nil
		} else {
			meta := make(map[string]string, len(info.Metadata))
			for k, v := range info.Metadata {
				meta[k] = v
			}
			lastChildMetadata = meta
		}
		childInfoMu.Unlock()
	}
}

func init() {
	registerRemoteActions()
	installRemoteChildHandler()
}

func installRemoteChildHandler() {
	handler := func(ctx context.Context, childNode gostage.ChildNode) error {
		diag := childNode.Diagnostics()
		go func(events <-chan diagnostics.Event) {
			for range events {
			}
		}(diag)
		defer childNode.Close()
		return childNode.Run(ctx)
	}
	childOpts := []gostage.ChildOption{
		gostage.WithChildPool(gostage.PoolConfig{
			Name:  remotePoolName,
			Tags:  []string{"remote"},
			Slots: 1,
		}),
		gostage.WithChildPool(gostage.PoolConfig{
			Name:  remoteAltPoolName,
			Tags:  []string{"remote", "alt"},
			Slots: 1,
		}),
	}
	gostage.HandleChild(handler, childOpts...)
	gostage.HandleChildNamed(remoteChildType, handler, childOpts...)
}

func TestMain(m *testing.M) {
	_ = modeFlag // ensure the flag is linked into the default FlagSet
	cfg, childMode, err := child.Detect(filterChildArgs(os.Args[1:]), os.Getenv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "remote e2e: detect child mode: %v\n", err)
		os.Exit(2)
	}
	if childMode {
		if err := runChildHarness(cfg); err != nil && !errors.Is(err, context.Canceled) {
			fmt.Fprintf(os.Stderr, "remote e2e: child harness error: %v\n", err)
			os.Exit(3)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func filterChildArgs(args []string) []string {
	out := make([]string, 0, len(args))
	for _, arg := range args {
		if strings.HasPrefix(arg, "-test.") {
			continue
		}
		out = append(out, arg)
	}
	return out
}

func runChildHarness(cfg child.Config) error {
	recordChildInfo(cfg)
	registerRemoteActions()
	node := child.NewNode(cfg)
	diag := node.Diagnostics()
	go func() {
		for range diag {
		}
	}()
	defer node.Close()
	if err := node.Run(context.Background()); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func registerRemoteActions() {
	gostage.MustRegisterAction("remote.success", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			return store.Put(ctx, "remote_success", true)
		}
	})

	gostage.MustRegisterAction("remote.wait", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ticker.C:
				}
			}
		}
	})
}

func remoteSuccessWorkflow() workflow.Definition {
	return workflow.Definition{
		Name: "RemoteSuccess",
		Tags: []string{"remote", "success"},
		Stages: []workflow.Stage{{
			Name: "success-stage",
			Tags: []string{"remote"},
			Actions: []workflow.Action{{
				Ref: "remote.success",
			}},
		}},
	}
}

func remoteCancelWorkflow() workflow.Definition {
	return workflow.Definition{
		Name: "RemoteCancel",
		Tags: []string{"remote", "cancel"},
		Stages: []workflow.Stage{{
			Name: "blocking-stage",
			Tags: []string{"remote"},
			Actions: []workflow.Action{{
				Ref: "remote.wait",
			}},
		}},
	}
}

func remoteOptions(t *testing.T, pools []child.PoolSpec) []gostage.Option {
	t.Helper()
	if len(pools) == 0 {
		t.Fatalf("remoteOptions requires at least one pool spec")
	}

	meta := make(map[string]string, len(remoteSpawnerMetadata))
	for k, v := range remoteSpawnerMetadata {
		meta[k] = v
	}

	spawnerCfg := gostage.SpawnerConfig{
		Name:          remoteSpawnerName,
		BinaryPath:    gostage.CurrentBinary(),
		Metadata:      meta,
		ChildType:     remoteChildType,
		MaxRestarts:   0,
		ShutdownGrace: 3 * time.Second,
	}

	options := []gostage.Option{gostage.WithSpawner(spawnerCfg)}
	for _, pool := range pools {
		cfg := gostage.PoolConfig{
			Name:    pool.Name,
			Tags:    append([]string{}, pool.Tags...),
			Slots:   int(pool.Slots),
			Spawner: spawnerCfg.Name,
		}
		if cfg.Slots <= 0 {
			cfg.Slots = 1
		}
		if len(cfg.Tags) == 0 {
			cfg.Tags = []string{"remote"}
		}
		options = append(options, gostage.WithPool(cfg))
	}

	return options
}

func waitForRemoteHealthy(t *testing.T, buf *testkit.HealthBuffer) {
	t.Helper()
	for {
		evt := buf.Next(t, string(node.HealthHealthy), 30*time.Second)
		if evt.Pool != remotePoolName {
			continue
		}
		if evt.Status != node.HealthHealthy {
			t.Fatalf("expected health status healthy, got %s", evt.Status)
		}
		return
	}
}

func killOrphans(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", "-lc", "timeout 5s make -C v3 kill-orphans")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	_ = cmd.Run()
}

func TestRemoteExecutionCompletes(t *testing.T) {
	resetChildInfo()
	killOrphans(t)
	testkit.ResetRegistry(t)
	registerRemoteActions()
	installRemoteChildHandler()

	workflowID, _ := gostage.MustRegisterWorkflow(remoteSuccessWorkflow())

	pools := []child.PoolSpec{
		{Name: remotePoolName, Slots: 1, Tags: []string{"remote"}},
		{Name: remoteAltPoolName, Slots: 1, Tags: []string{"remote", "alt"}},
	}
	backends := testkit.NewMemoryBackends()
	opts := append(testkit.MemoryOptions(backends), remoteOptions(t, pools)...)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, diagCh, err := gostage.Run(ctx, opts...)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	diag := testkit.StartDiagnosticsCollector(t, diagCh)
	t.Cleanup(func() {
		if t.Failed() {
			for _, evt := range diag.Events() {
				t.Logf("diag: component=%s severity=%s err=%v metadata=%v", evt.Component, evt.Severity, evt.Err, evt.Metadata)
			}
		}
		diag.Close()
	})

	telemetryBuf := testkit.StartTelemetryBuffer(ctx, t, node, 256)
	t.Cleanup(telemetryBuf.Close)

	healthBuf := testkit.StartHealthBuffer(ctx, t, node, 64)
	t.Cleanup(healthBuf.Close)

	t.Cleanup(func() { killOrphans(t) })
	t.Cleanup(func() { _ = node.Close() })

	waitForRemoteHealthy(t, healthBuf)

	runID, err := node.Submit(ctx,
		gostage.WorkflowRef(workflowID),
		gostage.WithTags("remote", "success"),
		gostage.WithInitialStore(map[string]any{"remote_seed": true}),
		gostage.WithMetadata(map[string]any{"request": "remote"}),
	)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	waitCtx, cancelWait := context.WithTimeout(ctx, 20*time.Second)
	defer cancelWait()
	var waitResult gostage.Result
	waitResult, err = node.Wait(waitCtx, runID)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if !waitResult.Success {
		t.Fatalf("expected success, got %+v", waitResult)
	}
	if waitResult.Reason != gostage.TerminationReasonSuccess {
		t.Fatalf("expected success reason, got %s", waitResult.Reason)
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("wait result: %+v", waitResult)
		}
	})

	startEvt := telemetryBuf.Next(t, telemetry.EventWorkflowStarted, 10*time.Second)
	if poolName, _ := startEvt.Metadata["pool"].(string); poolName == "" {
		t.Fatalf("expected pool metadata on workflow.started, got %+v", startEvt.Metadata)
	}

	execEvt := telemetryBuf.Next(t, telemetry.EventWorkflowExecution, 10*time.Second)
	if success, _ := execEvt.Metadata["success"].(bool); !success {
		t.Fatalf("expected telemetry success, got %+v", execEvt.Metadata)
	}

	summaryEvt := telemetryBuf.Next(t, telemetry.EventWorkflowSummary, 10*time.Second)
	if summaryEvt.Metadata["status"] != string(gostage.WorkflowCompleted) {
		t.Fatalf("unexpected summary telemetry: %+v", summaryEvt.Metadata)
	}
	if node.State == nil {
		t.Fatalf("expected state facade for remote test")
	}
	remoteSummary, err := node.State.WorkflowSummary(ctx, state.WorkflowID(runID))
	if err != nil {
		t.Fatalf("remote workflow summary: %v", err)
	}
	if _, ok := remoteSummary.Metadata["gostage.initial_store"]; ok {
		t.Fatalf("initial store leaked into remote metadata: %+v", remoteSummary.Metadata)
	}
	if remoteSummary.State != gostage.WorkflowCompleted {
		t.Fatalf("expected completed state, got %s", remoteSummary.State)
	}
	if remoteSummary.TerminationReason != gostage.TerminationReasonSuccess {
		t.Fatalf("expected termination reason success, got %s", remoteSummary.TerminationReason)
	}
	if !remoteSummary.Success {
		t.Fatalf("expected summary success flag, got %#v", remoteSummary)
	}
	if remoteSummary.CompletedAt == nil || remoteSummary.CompletedAt.IsZero() {
		t.Fatalf("expected completed_at timestamp, got %#v", remoteSummary.CompletedAt)
	}

	leftovers := telemetryBuf.Collect(t, 200*time.Millisecond)
	var dupStarted, dupExecution int
	for _, evt := range leftovers {
		switch evt.Kind {
		case telemetry.EventWorkflowStarted:
			dupStarted++
		case telemetry.EventWorkflowExecution:
			dupExecution++
		}
	}
	if dupStarted > 0 {
		t.Fatalf("duplicate workflow.started events observed (%d)", dupStarted+1)
	}
	if dupExecution > 0 {
		t.Fatalf("duplicate workflow.execution events observed (%d)", dupExecution+1)
	}

	harvestChildInfo(diag)
	expectChildInfo(t, remoteChildType, remoteSpawnerMetadata)

	for _, evt := range diag.Events() {
		if evt.Severity == diagnostics.SeverityError {
			t.Fatalf("diagnostic error: %+v", evt)
		}
	}
}

func TestRemoteExecutionCancellation(t *testing.T) {
	resetChildInfo()
	killOrphans(t)
	testkit.ResetRegistry(t)
	registerRemoteActions()
	installRemoteChildHandler()

	workflowID, _ := gostage.MustRegisterWorkflow(remoteCancelWorkflow())

	pools := []child.PoolSpec{
		{Name: remotePoolName, Slots: 1, Tags: []string{"remote"}},
		{Name: remoteAltPoolName, Slots: 1, Tags: []string{"remote", "alt"}},
	}
	backends := testkit.NewMemoryBackends()
	opts := append(testkit.MemoryOptions(backends), remoteOptions(t, pools)...)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, diagCh, err := gostage.Run(ctx, opts...)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	diag := testkit.StartDiagnosticsCollector(t, diagCh)
	t.Cleanup(func() {
		if t.Failed() {
			for _, evt := range diag.Events() {
				t.Logf("diag: component=%s severity=%s err=%v metadata=%v", evt.Component, evt.Severity, evt.Err, evt.Metadata)
			}
		}
		diag.Close()
	})

	telemetryBuf := testkit.StartTelemetryBuffer(ctx, t, node, 256)
	t.Cleanup(telemetryBuf.Close)

	healthBuf := testkit.StartHealthBuffer(ctx, t, node, 64)
	t.Cleanup(healthBuf.Close)

	t.Cleanup(func() { killOrphans(t) })
	t.Cleanup(func() { _ = node.Close() })

	waitForRemoteHealthy(t, healthBuf)

	runID, err := node.Submit(ctx,
		gostage.WorkflowRef(workflowID),
		gostage.WithTags("remote", "cancel"),
	)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	_ = telemetryBuf.Next(t, telemetry.EventWorkflowStarted, 10*time.Second)

	cancelCtx, cancelCancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancelCancel()
	if err := node.Cancel(cancelCtx, runID); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	waitCtx, cancelWait := context.WithTimeout(ctx, 20*time.Second)
	defer cancelWait()
	var waitResult gostage.Result
	waitResult, err = node.Wait(waitCtx, runID)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if waitResult.Success {
		t.Fatalf("expected cancellation failure, got %+v", waitResult)
	}
	if waitResult.Reason != gostage.TerminationReasonUserCancel {
		t.Fatalf("expected user cancel reason, got %s", waitResult.Reason)
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("wait result: %+v", waitResult)
		}
	})

	cancelEvt := telemetryBuf.Next(t, telemetry.EventWorkflowCancelled, 10*time.Second)
	if reason, _ := cancelEvt.Metadata["reason"].(string); reason != "explicit_request" && reason != string(gostage.TerminationReasonUserCancel) {
		t.Fatalf("unexpected cancel telemetry reason: %s", reason)
	}

	summaryEvt := telemetryBuf.Next(t, telemetry.EventWorkflowSummary, 10*time.Second)
	if summaryEvt.Metadata["status"] != string(gostage.WorkflowCancelled) {
		t.Fatalf("unexpected summary telemetry: %+v", summaryEvt.Metadata)
	}

	leftoversCancel := telemetryBuf.Collect(t, 200*time.Millisecond)
	var dupStartedCancel, dupExecCancel int
	for _, evt := range leftoversCancel {
		switch evt.Kind {
		case telemetry.EventWorkflowStarted:
			dupStartedCancel++
		case telemetry.EventWorkflowExecution:
			dupExecCancel++
		}
	}
	if dupStartedCancel > 0 {
		t.Fatalf("duplicate workflow.started events observed during cancel (%d)", dupStartedCancel+1)
	}
	if dupExecCancel > 0 {
		t.Fatalf("unexpected workflow.execution events during cancel (%d)", dupExecCancel)
	}

	harvestChildInfo(diag)
	expectChildInfo(t, remoteChildType, remoteSpawnerMetadata)

	for _, evt := range diag.Events() {
		if evt.Severity == diagnostics.SeverityError {
			t.Fatalf("diagnostic error: %+v", evt)
		}
	}
}
