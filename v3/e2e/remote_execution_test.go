package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
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
	_ "modernc.org/sqlite"
)

const (
	remoteSpawnerName  = "e2e-remote-spawner"
	remotePoolName     = "e2e-remote-pool"
	remoteAltPoolName  = "e2e-remote-pool-alt"
	remoteChildType    = "e2e-remote-child"
	childInfoLogPrefix = "CHILD-INFO"

	remoteControlStageID      = "remote-control-stage"
	remoteControlActionID     = "remote-control-action"
	remoteRemovableActionID   = "remote-removable-action"
	remoteSkipStageID         = "remote-skip-stage"
	remoteSkipActionID        = "remote-skip-action"
	remoteDynamicStageStoreID = "remote_dynamic_stage_id"
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
	registerLifecycleActions()
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
			ctx.Logger().Info("remote success", map[string]any{"workflow": ctx.Workflow().ID()})
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

	gostage.MustRegisterAction("remote.fail", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			ctx.Logger().Error("remote failure", map[string]any{"workflow": ctx.Workflow().ID()})
			return fmt.Errorf("remote failure")
		}
	})

	gostage.MustRegisterAction("remote.control", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			if err := store.Put(ctx, "remote_control_ran", true); err != nil {
				return err
			}
			ctx.Actions().Remove(remoteRemovableActionID)
			dynStage := workflow.NewRuntimeStage("remote-dynamic-stage", "remote dynamic", "")
			dynStage.AddActions(workflow.MustRuntimeAction("remote.dynamic.noop"))
			dynID := ctx.Stages().Add(dynStage)
			ctx.Stages().Remove(dynID)
			if err := store.Put(ctx, remoteDynamicStageStoreID, dynID); err != nil {
				return err
			}
			ctx.Stages().Disable(remoteSkipStageID)
			ctx.Actions().Disable(remoteSkipActionID)
			return nil
		}
	})

	gostage.MustRegisterAction("remote.removable", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			return store.Put(ctx, "remote_removable_ran", true)
		}
	})

	gostage.MustRegisterAction("remote.skip", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			return store.Put(ctx, "remote_skip_ran", true)
		}
	})

	gostage.MustRegisterAction("remote.dynamic.noop", func() gostage.ActionFunc {
		return func(ctx rt.Context) error { return nil }
	})

	gostage.MustRegisterAction("remote.retry.once", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			wfStore := ctx.Workflow().Store()
			if wfStore.IsZero() {
				wfStore = ctx.Store()
			}

			if _, err := store.Get[string](wfStore, "seed"); err != nil {
				return fmt.Errorf("missing seed: %w", err)
			}

			attempt, err := store.Get[int](wfStore, "attempt_count")
			if err != nil {
				if attemptFloat, ferr := store.Get[float64](wfStore, "attempt_count"); ferr == nil {
					attempt = int(attemptFloat)
				} else {
					attempt = 0
				}
			}
			attempt++
			if err := store.Put(wfStore, "attempt_count", attempt); err != nil {
				return err
			}

			failNext, err := store.Get[bool](wfStore, "fail_next")
			if err != nil {
				if failFloat, ferr := store.Get[float64](wfStore, "fail_next"); ferr == nil {
					failNext = failFloat != 0
				} else {
					failNext = false
				}
			}
			if failNext {
				if err := store.Put(wfStore, "fail_next", false); err != nil {
					return err
				}
				return fmt.Errorf("remote retry required")
			}

			if err := store.Put(wfStore, "final_attempt", attempt); err != nil {
				return err
			}
			return nil
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

func remoteFailureWorkflow() workflow.Definition {
	return workflow.Definition{
		Name: "RemoteFailure",
		Tags: []string{"remote", "failure"},
		Stages: []workflow.Stage{{
			Name: "failure-stage",
			Tags: []string{"remote"},
			Actions: []workflow.Action{{
				Ref: "remote.fail",
			}},
		}},
	}
}

func remoteMutationWorkflow() workflow.Definition {
	return workflow.Definition{
		Name: "RemoteMutations",
		Tags: []string{"remote", "mutate"},
		Stages: []workflow.Stage{
			{
				ID:   remoteControlStageID,
				Name: "control-stage",
				Tags: []string{"remote"},
				Actions: []workflow.Action{
					{ID: remoteControlActionID, Ref: "remote.control"},
					{ID: remoteRemovableActionID, Ref: "remote.removable"},
				},
			},
			{
				ID:   remoteSkipStageID,
				Name: "skip-stage",
				Tags: []string{"remote"},
				Actions: []workflow.Action{{
					ID:  remoteSkipActionID,
					Ref: "remote.skip",
				}},
			},
		},
	}
}

func remoteRetryWorkflow() workflow.Definition {
	return workflow.Definition{
		Name: "RemoteRetry",
		Tags: []string{"remote", "retry"},
		Stages: []workflow.Stage{{
			Name: "retry-stage",
			Tags: []string{"remote"},
			Actions: []workflow.Action{{
				Ref: "remote.retry.once",
			}},
		}},
	}
}

func remoteOptions(t *testing.T, pools []child.PoolSpec, mutate ...func(*gostage.SpawnerConfig)) []gostage.Option {
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
	for _, fn := range mutate {
		if fn != nil {
			fn(&spawnerCfg)
		}
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
	cases := []struct {
		name                 string
		mutate               func(*gostage.SpawnerConfig)
		expectCapturedStdout bool
	}{
		{
			name:                 "stdout-capture",
			expectCapturedStdout: true,
		},
		{
			name: "disable-output-capture",
			mutate: func(cfg *gostage.SpawnerConfig) {
				cfg.DisableOutputCapture = true
			},
			expectCapturedStdout: false,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
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
			opts := append(testkit.MemoryOptions(backends), remoteOptions(t, pools, tc.mutate)...)

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

			var events []diagnostics.Event
			deadline := time.Now().Add(2 * time.Second)
			for {
				events = diag.Events()
				seen := false
				for _, evt := range events {
					if strings.HasPrefix(evt.Component, "remote.log") {
						seen = true
						break
					}
				}
				if seen || time.Now().After(deadline) {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			structuredLogSeen := false
			bootstrapSeen := false
			lineEvents := 0
			for _, evt := range events {
				if strings.HasPrefix(evt.Component, "remote.log") {
					structuredLogSeen = true
					if evt.Metadata["message"] != "remote success" {
						t.Fatalf("unexpected structured log metadata: %+v", evt.Metadata)
					}
					if evt.Metadata["structured"] != true {
						t.Fatalf("expected structured flag, got %+v", evt.Metadata)
					}
				}
				if evt.Component == "child.bootstrap" {
					bootstrapSeen = true
					if evt.Metadata["child_type"] != remoteChildType {
						t.Fatalf("unexpected child bootstrap metadata: %+v", evt.Metadata)
					}
				}
				if _, ok := evt.Metadata["line"]; ok {
					lineEvents++
				}
			}
			if !structuredLogSeen {
				t.Fatalf("expected structured log event, diagnostics=%+v", events)
			}
			if !bootstrapSeen {
				t.Fatalf("expected child bootstrap diagnostic event, diagnostics=%+v", events)
			}
			if tc.expectCapturedStdout && lineEvents == 0 {
				t.Fatalf("expected captured stdout diagnostics, got none")
			}
			if !tc.expectCapturedStdout && lineEvents > 0 {
				t.Fatalf("expected stdout capture disabled, saw %d line events", lineEvents)
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

			if tc.expectCapturedStdout {
				harvestChildInfo(diag)
				expectChildInfo(t, remoteChildType, remoteSpawnerMetadata)
			}

			for _, evt := range events {
				if evt.Severity == diagnostics.SeverityError {
					t.Fatalf("diagnostic error: %+v", evt)
				}
			}
		})
	}
}

func TestRemoteExecutionFailureAndMutations(t *testing.T) {
	resetChildInfo()
	killOrphans(t)
	testkit.ResetRegistry(t)
	registerRemoteActions()
	installRemoteChildHandler()

	failureWorkflowID, _ := gostage.MustRegisterWorkflow(remoteFailureWorkflow())
	mutationsWorkflowID, _ := gostage.MustRegisterWorkflow(remoteMutationWorkflow())

	pools := []child.PoolSpec{
		{Name: remotePoolName, Slots: 1, Tags: []string{"remote"}},
		{Name: remoteAltPoolName, Slots: 1, Tags: []string{"remote", "alt"}},
	}

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "remote-mutations.db")
	dsn := fmt.Sprintf("file:%s?_busy_timeout=15000&_foreign_keys=on&_journal_mode=WAL", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })

	options := []gostage.Option{
		gostage.WithSQLite(gostage.SQLiteConfig{
			DB:              db,
			ApplyMigrations: true,
		}),
	}
	options = append(options, remoteOptions(t, pools)...)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, diagCh, err := gostage.Run(ctx, options...)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	diag := testkit.StartDiagnosticsCollector(t, diagCh)
	telemetryBuf := testkit.StartTelemetryBuffer(ctx, t, node, 512)
	healthBuf := testkit.StartHealthBuffer(ctx, t, node, 64)

	t.Cleanup(func() { killOrphans(t) })
	t.Cleanup(func() { telemetryBuf.Close() })
	t.Cleanup(func() { healthBuf.Close() })
	t.Cleanup(func() { diag.Close() })
	t.Cleanup(func() { _ = node.Close() })

	waitForRemoteHealthy(t, healthBuf)
	t.Logf("diag events after health: %d", len(diag.Events()))

	waitEventForRun := func(kind telemetry.EventKind, workflowID gostage.WorkflowID) telemetry.Event {
		t.Helper()
		for {
			evt := telemetryBuf.Next(t, kind, 10*time.Second)
			if evt.WorkflowID == string(workflowID) {
				return evt
			}
		}
	}

	// Failure scenario
	failureRunID, err := node.Submit(ctx,
		gostage.WorkflowRef(failureWorkflowID),
		gostage.WithTags("remote", "failure"),
	)
	if err != nil {
		t.Fatalf("submit failure workflow: %v", err)
	}

	failureWaitCtx, cancelFailureWait := context.WithTimeout(ctx, 20*time.Second)
	defer cancelFailureWait()
	failureResult, err := node.Wait(failureWaitCtx, failureRunID)
	if err != nil {
		t.Fatalf("wait failure workflow: %v", err)
	}
	if failureResult.Success {
		t.Fatalf("expected failure workflow to fail, got success result: %+v", failureResult)
	}
	if failureResult.Reason != gostage.TerminationReasonFailure {
		t.Fatalf("expected failure termination reason, got %s", failureResult.Reason)
	}

	execFailEvt := waitEventForRun(telemetry.EventWorkflowExecution, failureRunID)
	if success, _ := execFailEvt.Metadata["success"].(bool); success {
		t.Fatalf("expected telemetry execution metadata success=false, got %+v", execFailEvt.Metadata)
	}

	summaryFailEvt := waitEventForRun(telemetry.EventWorkflowSummary, failureRunID)
	if summaryFailEvt.Metadata["status"] != string(gostage.WorkflowFailed) {
		t.Fatalf("unexpected failure summary telemetry: %+v", summaryFailEvt.Metadata)
	}

	var dbState, dbReason string
	if err := db.QueryRowContext(ctx,
		`SELECT state, termination_reason FROM workflow_runs WHERE id = ?`, failureRunID,
	).Scan(&dbState, &dbReason); err != nil {
		t.Fatalf("query failure run state: %v", err)
	}
	if dbState != string(gostage.WorkflowFailed) {
		t.Fatalf("expected workflow_runs state failed, got %s", dbState)
	}
	if dbReason != string(gostage.TerminationReasonFailure) {
		t.Fatalf("unexpected termination reason %s", dbReason)
	}

	// Drain any remaining telemetry from failure run.
	telemetryBuf.Collect(t, 200*time.Millisecond)

	// Mutation scenario
	mutationsRunID, err := node.Submit(ctx,
		gostage.WorkflowRef(mutationsWorkflowID),
		gostage.WithTags("remote", "mutate"),
	)
	if err != nil {
		t.Fatalf("submit mutation workflow: %v", err)
	}

	mutateWaitCtx, cancelMutateWait := context.WithTimeout(ctx, 20*time.Second)
	defer cancelMutateWait()
	mutationsResult, err := node.Wait(mutateWaitCtx, mutationsRunID)
	if err != nil {
		t.Fatalf("wait mutation workflow: %v", err)
	}
	if !mutationsResult.Success {
		t.Fatalf("expected mutation workflow to succeed, got %+v", mutationsResult)
	}

	if _, ok := mutationsResult.Output["remote_removable_ran"]; ok {
		t.Fatalf("removed action should not have run, output=%+v", mutationsResult.Output)
	}
	if _, ok := mutationsResult.Output["remote_skip_ran"]; ok {
		t.Fatalf("skipped action should not have run, output=%+v", mutationsResult.Output)
	}
	controlFlag, ok := mutationsResult.Output["remote_control_ran"].(bool)
	if !ok || !controlFlag {
		t.Fatalf("control action did not record execution, output=%+v", mutationsResult.Output)
	}
	dynamicStageValue, ok := mutationsResult.Output[remoteDynamicStageStoreID].(string)
	if !ok || dynamicStageValue == "" {
		t.Fatalf("missing dynamic stage id in output: %+v", mutationsResult.Output)
	}

	summaryMutateEvt := waitEventForRun(telemetry.EventWorkflowSummary, mutationsRunID)
	if summaryMutateEvt.Metadata["status"] != string(gostage.WorkflowCompleted) {
		t.Fatalf("unexpected mutation summary telemetry: %+v", summaryMutateEvt.Metadata)
	}

	if !mutationsResult.DisabledStages[remoteSkipStageID] {
		t.Fatalf("expected skip stage to be disabled, result=%+v", mutationsResult.DisabledStages)
	}
	disabledActionKey := remoteSkipActionID
	if !mutationsResult.DisabledActions[disabledActionKey] {
		t.Fatalf("expected skip action to be disabled, result=%+v", mutationsResult.DisabledActions)
	}
	removedActionKey := remoteControlStageID + "::" + remoteRemovableActionID
	if _, ok := mutationsResult.RemovedActions[removedActionKey]; !ok {
		t.Fatalf("expected removable action recorded as removed, result=%+v", mutationsResult.RemovedActions)
	}
	if _, ok := mutationsResult.RemovedStages[dynamicStageValue]; !ok {
		t.Fatalf("expected dynamic stage recorded as removed, result=%+v", mutationsResult.RemovedStages)
	}

	var disabledStagesJSON, disabledActionsJSON, removedStagesJSON, removedActionsJSON []byte
	if err := db.QueryRowContext(ctx,
		`SELECT disabled_stages, disabled_actions, removed_stages, removed_actions FROM execution_summaries WHERE workflow_id = ?`,
		mutationsRunID,
	).Scan(&disabledStagesJSON, &disabledActionsJSON, &removedStagesJSON, &removedActionsJSON); err != nil {
		t.Fatalf("query execution summary for mutation run: %v", err)
	}

	decodeMap := func(data []byte) map[string]any {
		out := make(map[string]any)
		if len(data) == 0 {
			return out
		}
		if err := json.Unmarshal(data, &out); err != nil {
			t.Fatalf("decode json map: %v", err)
		}
		return out
	}

	disabledStagesMap := decodeMap(disabledStagesJSON)
	disabledActionsMap := decodeMap(disabledActionsJSON)
	removedStagesMap := decodeMap(removedStagesJSON)
	removedActionsMap := decodeMap(removedActionsJSON)

	if _, ok := disabledStagesMap[remoteSkipStageID]; !ok {
		t.Fatalf("expected disabledStages persisted to include %s: %#v", remoteSkipStageID, disabledStagesMap)
	}
	if _, ok := disabledActionsMap[disabledActionKey]; !ok {
		t.Fatalf("expected disabledActions persisted to include %s: %#v", disabledActionKey, disabledActionsMap)
	}
	if _, ok := removedStagesMap[dynamicStageValue]; !ok {
		t.Fatalf("expected removedStages persisted to include %s: %#v", dynamicStageValue, removedStagesMap)
	}
	if _, ok := removedActionsMap[removedActionKey]; !ok {
		t.Fatalf("expected removedActions persisted to include %s: %#v", removedActionKey, removedActionsMap)
	}

	summary, err := node.State.WorkflowSummary(ctx, state.WorkflowID(mutationsRunID))
	if err != nil {
		t.Fatalf("state summary for mutation run: %v", err)
	}
	actionHistory, err := node.State.ActionHistory(ctx, state.WorkflowID(mutationsRunID))
	if err != nil {
		t.Fatalf("action history for mutation run: %v", err)
	}
	expectStageStatus := func(stageID string, expected state.WorkflowState) {
		if summary.Stages != nil {
			if rec, ok := summary.Stages[stageID]; ok {
				if rec.Status != expected {
					t.Fatalf("expected stage %s status=%s, got %s", stageID, expected, rec.Status)
				}
				return
			}
		}
		var dbStatus string
		if err := db.QueryRowContext(ctx,
			`SELECT state FROM stage_runs WHERE workflow_id = ? AND stage_id = ?`,
			mutationsRunID, stageID,
		).Scan(&dbStatus); err != nil {
			t.Fatalf("query stage_runs for %s: %v", stageID, err)
		}
		if dbStatus != string(expected) {
			t.Fatalf("expected stage %s status=%s in DB, got %s", stageID, expected, dbStatus)
		}
	}
	expectActionStatus := func(stageID, actionID string, expected state.WorkflowState) {
		if summary.Stages != nil {
			if stageRec, ok := summary.Stages[stageID]; ok && stageRec.Actions != nil {
				if actionRec, ok := stageRec.Actions[actionID]; ok {
					if actionRec.Status != expected {
						t.Fatalf("expected action %s/%s status=%s, got %s", stageID, actionID, expected, actionRec.Status)
					}
					return
				}
			}
		}
		for _, rec := range actionHistory {
			if rec.StageID == stageID && rec.ActionID == actionID {
				if rec.State != expected {
					t.Fatalf("expected action %s/%s state=%s, got %s", stageID, actionID, expected, rec.State)
				}
				return
			}
		}
		var dbStatus string
		if err := db.QueryRowContext(ctx,
			`SELECT state FROM action_runs WHERE workflow_id = ? AND stage_id = ? AND action_id = ?`,
			mutationsRunID, stageID, actionID,
		).Scan(&dbStatus); err != nil {
			t.Fatalf("query action_runs for %s/%s: %v", stageID, actionID, err)
		}
		if dbStatus != string(expected) {
			t.Fatalf("expected action %s/%s status=%s in DB, got %s", stageID, actionID, expected, dbStatus)
		}
	}
	expectStageStatus(remoteSkipStageID, state.WorkflowSkipped)
	expectActionStatus(remoteSkipStageID, remoteSkipActionID, state.WorkflowSkipped)
	expectActionStatus(remoteControlStageID, remoteRemovableActionID, state.WorkflowRemoved)
	expectStageStatus(dynamicStageValue, state.WorkflowRemoved)

	if events := diag.Events(); len(events) > 0 {
		for _, evt := range events {
			if evt.Severity == diagnostics.SeverityError || evt.Severity == diagnostics.SeverityCritical {
				if strings.HasPrefix(evt.Component, "remote.log.") {
					continue
				}
				t.Fatalf("diagnostic error: %+v", evt)
			}
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
	if node.State == nil {
		t.Fatalf("expected state reader on node")
	}

	_ = telemetryBuf.Next(t, telemetry.EventWorkflowStarted, 10*time.Second)
	testkit.WaitForWorkflowInState(t, node.State, state.WorkflowID(runID), state.WorkflowRunning)

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

func TestRemoteFailurePolicyRetry(t *testing.T) {
	resetChildInfo()
	killOrphans(t)
	testkit.ResetRegistry(t)
	registerRemoteActions()
	installRemoteChildHandler()

	workflowID, _ := gostage.MustRegisterWorkflow(remoteRetryWorkflow())

	pools := []child.PoolSpec{
		{Name: remotePoolName, Slots: 1, Tags: []string{"remote"}},
	}
	backends := testkit.NewMemoryBackends()
	failurePolicy := gostage.FailurePolicyFunc(func(_ context.Context, info gostage.FailureContext) gostage.FailureOutcome {
		if info.Attempt < 2 {
			return gostage.RetryOutcome()
		}
		return gostage.AckOutcome()
	})
	opts := append(testkit.MemoryOptions(backends), remoteOptions(t, pools)...)
	opts = append(opts, gostage.WithFailurePolicy(failurePolicy))

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
		gostage.WithTags("remote", "retry"),
		gostage.WithInitialStore(map[string]any{"seed": "value", "fail_next": true}),
	)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	waitCtx, cancelWait := context.WithTimeout(ctx, 20*time.Second)
	defer cancelWait()
	result, err := node.Wait(waitCtx, runID)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	coerceInt := func(val any) (int, bool) {
		switch v := val.(type) {
		case int:
			return v, true
		case int32:
			return int(v), true
		case int64:
			return int(v), true
		case float64:
			return int(v), true
		default:
			return 0, false
		}
	}
	coerceBool := func(val any) (bool, bool) {
		switch v := val.(type) {
		case bool:
			return v, true
		case int:
			return v != 0, true
		case int32:
			return v != 0, true
		case int64:
			return v != 0, true
		case float64:
			return v != 0, true
		default:
			return false, false
		}
	}
	if !result.Success {
		t.Fatalf("expected success after retry, got %+v", result)
	}
	if result.Attempt != 2 {
		t.Fatalf("expected attempt 2, got %d", result.Attempt)
	}
	if val, ok := coerceInt(result.Output["final_attempt"]); !ok || val != 2 {
		t.Fatalf("expected final_attempt=2, got %+v", result.Output["final_attempt"])
	}
	if val, ok := coerceInt(result.Output["attempt_count"]); !ok || val != 2 {
		t.Fatalf("expected attempt_count=2, got %+v", result.Output["attempt_count"])
	}
	if seed, ok := result.Output["seed"].(string); !ok || seed != "value" {
		t.Fatalf("expected seed value retained, got %+v", result.Output["seed"])
	}
	if failNext, ok := coerceBool(result.Output["fail_next"]); !ok || failNext {
		t.Fatalf("expected fail_next=false after retry, got %+v", result.Output["fail_next"])
	}

	startEvt := telemetryBuf.Next(t, telemetry.EventWorkflowStarted, 10*time.Second)
	if startEvt.WorkflowID != string(runID) {
		t.Fatalf("unexpected workflow.started event: %+v", startEvt)
	}
	firstExec := telemetryBuf.Next(t, telemetry.EventWorkflowExecution, 10*time.Second)
	if success, _ := firstExec.Metadata["success"].(bool); success {
		t.Fatalf("expected first execution to fail, got %+v", firstExec.Metadata)
	}
	retryEvt := telemetryBuf.Next(t, telemetry.EventWorkflowRetry, 10*time.Second)
	if retryEvt.WorkflowID != string(runID) {
		t.Fatalf("unexpected workflow.retry event: %+v", retryEvt)
	}
	secondExec := telemetryBuf.Next(t, telemetry.EventWorkflowExecution, 10*time.Second)
	if success, _ := secondExec.Metadata["success"].(bool); !success {
		t.Fatalf("expected second execution success, got %+v", secondExec.Metadata)
	}
	summaryEvt := telemetryBuf.Next(t, telemetry.EventWorkflowSummary, 10*time.Second)
	if summaryEvt.Metadata["status"] != string(gostage.WorkflowCompleted) {
		t.Fatalf("unexpected summary telemetry: %+v", summaryEvt.Metadata)
	}
	if attemptVal, ok := summaryEvt.Metadata["attempt"]; ok {
		if attempt, ok := coerceInt(attemptVal); ok && attempt != 2 {
			t.Fatalf("expected telemetry attempt=2, got %+v", attemptVal)
		}
	}

	summary, err := node.State.WorkflowSummary(ctx, state.WorkflowID(runID))
	if err != nil {
		t.Fatalf("workflow summary: %v", err)
	}
	if summary.State != state.WorkflowCompleted {
		t.Fatalf("expected summary state completed, got %s", summary.State)
	}
	if summary.TerminationReason != state.TerminationReasonSuccess {
		t.Fatalf("expected summary termination reason success, got %s", summary.TerminationReason)
	}
	if !summary.Success {
		t.Fatalf("expected summary success flag true")
	}
	if len(summary.Stages) == 0 {
		t.Fatalf("expected summary stages, got %+v", summary.Stages)
	}
	for stageID, rec := range summary.Stages {
		if rec == nil {
			continue
		}
		if rec.Status != state.WorkflowCompleted {
			t.Fatalf("expected summary stage %s completed, got %s", stageID, rec.Status)
		}
		for actionID, action := range rec.Actions {
			if action == nil {
				continue
			}
			if action.Status != state.WorkflowCompleted {
				t.Fatalf("expected summary action %s/%s completed, got %s", stageID, actionID, action.Status)
			}
		}
	}
	history, err := node.State.ActionHistory(ctx, state.WorkflowID(runID))
	if err != nil {
		t.Fatalf("action history: %v", err)
	}
	if len(history) == 0 {
		t.Fatalf("expected action history entries")
	}
	var completedHistory bool
	for _, rec := range history {
		if rec.State == state.WorkflowCompleted {
			completedHistory = true
			break
		}
	}
	if !completedHistory {
		t.Fatalf("expected at least one completed action history entry, got %+v", history)
	}

	if snap := backends.Observer.Snapshot(); snap.Summaries != nil {
		if sum, ok := snap.Summaries[runID]; ok {
			if sum.Attempt != 2 {
				t.Fatalf("observer attempt mismatch: %d", sum.Attempt)
			}
			if stages := snap.Stages[runID]; len(stages) == 0 {
				t.Fatalf("expected recorded stages for %s", runID)
			} else {
				for _, stage := range stages {
					if stage.Status != state.WorkflowCompleted {
						t.Fatalf("observer stage not completed: %+v", stage)
					}
				}
			}
			if actions := snap.Actions[runID]; len(actions) == 0 {
				t.Fatalf("expected recorded actions for %s", runID)
			} else {
				for key, action := range actions {
					if action.State != state.WorkflowCompleted {
						t.Fatalf("observer action %s not completed: %+v", key, action)
					}
				}
			}
		} else {
			t.Fatalf("observer missing summary for %s", runID)
		}
	}
}
