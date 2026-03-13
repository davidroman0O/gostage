package gostage

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	pb "github.com/davidroman0O/gostage/proto"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// spawnServer manages the gRPC server and job dispatch for spawned ForEach.
// The parent process runs this server; child processes connect as clients.
type spawnServer struct {
	pb.UnimplementedWorkflowIPCServer

	mu   sync.Mutex
	jobs map[string]*SpawnJob // job ID → job

	listener net.Listener
	srv      *grpc.Server
	port     int

	// engine reference for routing child IPC messages to handlers
	engine *Engine

	secret string
}

// SpawnJob represents one ForEach item to execute in a child process.
// The exported fields are accessible to ChildMiddleware implementations.
type SpawnJob struct {
	ID             string
	RunID          RunID             // the run this job belongs to
	TaskName       string
	StoreData      map[string][]byte // serialized store snapshot including item/index
	DefinitionJSON []byte            // serialized SubWorkflowDef for multi-stage children
	resultCh       chan *spawnResult  // child sends result here (buffered, size 1)
	token          string            // per-job auth token
}

// spawnResult holds the outcome of a child process execution.
type spawnResult struct {
	storeData  map[string][]byte // final store from child
	err        string            // error message, empty on success
	bailReason string            // bail reason if the child bailed, empty otherwise
}

// newSpawnServer creates and starts a gRPC server on a random available port.
func newSpawnServer(engine *Engine, secret string) (*spawnServer, error) {
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}

	authInterceptor := grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Errorf(codes.Unauthenticated, "missing metadata")
		}
		tokens := md.Get("x-gostage-secret")
		if len(tokens) == 0 || subtle.ConstantTimeCompare([]byte(tokens[0]), []byte(secret)) != 1 {
			return nil, status.Errorf(codes.Unauthenticated, "invalid secret")
		}
		return handler(ctx, req)
	})

	ss := &spawnServer{
		jobs:     make(map[string]*SpawnJob),
		listener: lis,
		srv: grpc.NewServer(
			authInterceptor,
			grpc.KeepaliveParams(keepalive.ServerParameters{
				Time:    10 * time.Second,
				Timeout: 5 * time.Second,
			}),
			grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
				MinTime:             5 * time.Second,
				PermitWithoutStream: true,
			}),
		),
		port:   lis.Addr().(*net.TCPAddr).Port,
		engine: engine,
		secret: secret,
	}

	pb.RegisterWorkflowIPCServer(ss.srv, ss)

	go ss.srv.Serve(lis)

	return ss, nil
}

// stop gracefully shuts down the gRPC server.
func (ss *spawnServer) stop() {
	ss.srv.GracefulStop()
}

// addJob registers a job for a child process to pick up.
func (ss *spawnServer) addJob(job *SpawnJob) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.jobs[job.ID] = job
}

// RequestWorkflowDefinition is called by child processes to get their work assignment.
func (ss *spawnServer) RequestWorkflowDefinition(ctx context.Context, req *pb.ReadySignal) (*pb.WorkflowDefinition, error) {
	ss.mu.Lock()
	job, ok := ss.jobs[req.ChildId]
	ss.mu.Unlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "job %q not found", req.ChildId)
	}

	// Validate per-job token
	md, _ := metadata.FromIncomingContext(ctx)
	jobTokens := md.Get("x-gostage-job-token")
	if len(jobTokens) == 0 || subtle.ConstantTimeCompare([]byte(jobTokens[0]), []byte(job.token)) != 1 {
		return nil, status.Errorf(codes.PermissionDenied, "invalid job token")
	}

	return &pb.WorkflowDefinition{
		Id:             job.ID,
		Name:           job.TaskName,
		InitialStore:   job.StoreData,
		DefinitionJson: job.DefinitionJSON,
	}, nil
}

// SendMessage handles messages from child processes.
// Used for final store delivery and IPC messages.
func (ss *spawnServer) SendMessage(ctx context.Context, msg *pb.IPCMessage) (*pb.MessageAck, error) {
	jobID := ""
	if msg.Context != nil {
		jobID = msg.Context.SessionId
	}

	// Validate per-job token unconditionally: reject empty job IDs and invalid tokens
	if jobID == "" {
		return nil, status.Errorf(codes.InvalidArgument, "missing job identifier")
	}
	md, _ := metadata.FromIncomingContext(ctx)
	jobTokens := md.Get("x-gostage-job-token")
	ss.mu.Lock()
	job, ok := ss.jobs[jobID]
	ss.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "job %q not found", jobID)
	}
	if len(jobTokens) == 0 || subtle.ConstantTimeCompare([]byte(jobTokens[0]), []byte(job.token)) != 1 {
		return nil, status.Errorf(codes.PermissionDenied, "invalid job token for job %q", jobID)
	}

	switch msg.Type {
	case pb.MessageType_MESSAGE_TYPE_FINAL_STORE:
		if payload := msg.GetFinalStore(); payload != nil {
			ss.mu.Lock()
			job, ok := ss.jobs[jobID]
			ss.mu.Unlock()
			if ok {
				select {
				case job.resultCh <- &spawnResult{storeData: payload.StoreData}:
				default:
				}
			}
		}

	case pb.MessageType_MESSAGE_TYPE_WORKFLOW_RESULT:
		if payload := msg.GetWorkflowResult(); payload != nil {
			if !payload.Success {
				ss.mu.Lock()
				job, ok := ss.jobs[jobID]
				ss.mu.Unlock()
				if ok {
					select {
					case job.resultCh <- &spawnResult{err: payload.ErrorMessage}:
					default:
					}
				}
			}
		}

	case pb.MessageType_MESSAGE_TYPE_BAIL:
		if payload := msg.GetBailPayload(); payload != nil {
			ss.mu.Lock()
			job, ok := ss.jobs[jobID]
			ss.mu.Unlock()
			if ok {
				select {
				case job.resultCh <- &spawnResult{bailReason: payload.Reason}:
				default:
				}
			}
		}

	case pb.MessageType_MESSAGE_TYPE_IPC:
		// Dedicated IPC message type — route to engine message handlers.
		if payload := msg.GetIpcPayload(); payload != nil && ss.engine != nil {
			var parsed any
			if err := json.Unmarshal(payload.PayloadJson, &parsed); err == nil {
				payloadMap, ok := parsed.(map[string]any)
				if !ok {
					payloadMap = map[string]any{"data": parsed}
				}
				jobRunID := RunID("")
				ss.mu.Lock()
				if job, ok := ss.jobs[jobID]; ok {
					jobRunID = job.RunID
				}
				ss.mu.Unlock()
				ss.engine.dispatchMessage(payload.MsgType, payloadMap, jobRunID)
			}
		}

	default:
		// Unknown message type — ignore
	}

	return &pb.MessageAck{Success: true, MessageId: msg.MessageId}, nil
}

// executeForEachSpawn runs ForEach items in isolated child processes.
func (e *Engine) executeForEachSpawn(ctx context.Context, wf *Workflow, s *step, items []any, runID RunID, resuming bool) error {
	secret := uuid.New().String()
	ss, err := newSpawnServer(e, secret)
	if err != nil {
		return fmt.Errorf("start spawn server: %w", err)
	}
	defer ss.stop()

	// Load step states for per-item resume tracking
	var stepStates map[string]Status
	if resuming {
		run, loadErr := e.persistence.LoadRun(ctx, runID)
		if loadErr == nil && run.StepStates != nil {
			stepStates = run.StepStates
		}
	}

	concurrency := s.concurrency
	if concurrency < 1 {
		concurrency = 1
	}

	sem := make(chan struct{}, concurrency)
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)

	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i, item := range items {
		// Skip completed items on resume
		itemKey := fmt.Sprintf("%s:%d", s.id, i)
		if resuming && stepStates != nil && stepStates[itemKey] == Completed {
			continue
		}

		// Check for prior error or cancellation
		mu.Lock()
		hasErr := firstErr != nil
		mu.Unlock()
		if hasErr || childCtx.Err() != nil {
			break
		}

		// Serialize state with this item
		storeData, serErr := serializeStateForChild(wf.state, item, i)
		if serErr != nil {
			return fmt.Errorf("serialize state for item %d: %w", i, serErr)
		}

		jobID := uuid.New().String()
		jobToken := uuid.New().String()
		job := &SpawnJob{
			ID:        jobID,
			RunID:     runID,
			TaskName:  s.forEachRef.taskName,
			StoreData: storeData,
			resultCh:  make(chan *spawnResult, 1),
			token:     jobToken,
		}

		// For sub-workflow refs, serialize the workflow definition for child transfer
		if s.forEachRef.subWorkflow != nil {
			def, defErr := WorkflowToDefinition(s.forEachRef.subWorkflow)
			if defErr != nil {
				return fmt.Errorf("serialize workflow definition: %w", defErr)
			}
			defJSON, marshalErr := MarshalWorkflowDefinition(def)
			if marshalErr != nil {
				return fmt.Errorf("marshal workflow definition: %w", marshalErr)
			}
			job.DefinitionJSON = defJSON
			job.TaskName = s.forEachRef.subWorkflow.ID
		}
		ss.addJob(job)

		sem <- struct{}{} // acquire semaphore
		wg.Add(1)

		go func(job *SpawnJob, itemIndex int, itemStepKey string) {
			defer wg.Done()
			defer func() { <-sem }() // release semaphore
			defer func() {
				if r := recover(); r != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("spawn item %d panic: %v", itemIndex, r)
						cancel()
					}
					mu.Unlock()
				}
			}()

			// Execute spawn with middleware chain
			var result *spawnResult
			var spawnErr error

			doSpawn := func() error {
				result, spawnErr = spawnChild(childCtx, ss.port, job, ss.secret)
				if spawnErr != nil {
					return spawnErr
				}
				if result.bailReason != "" {
					return &BailError{Reason: result.bailReason}
				}
				if result.err != "" {
					return fmt.Errorf("%s", result.err)
				}
				return nil
			}

			var chainErr error
			if len(e.childMiddleware) > 0 {
				chain := doSpawn
				for j := 0; j < len(e.childMiddleware); j++ {
					mw := e.childMiddleware[j]
					next := chain
					chain = func() error {
						return mw(childCtx, job, next)
					}
				}
				chainErr = func() (rerr error) {
					defer func() {
						if r := recover(); r != nil {
							rerr = fmt.Errorf("middleware panic: %v", r)
						}
					}()
					return chain()
				}()
			} else {
				chainErr = doSpawn()
			}

			if chainErr != nil {
				mu.Lock()
				if firstErr == nil {
					// Preserve BailError without wrapping so it propagates
					// through executeForEachSpawn → executeWorkflow → doExecute
					// with Bailed status intact.
					var bailErr *BailError
					if errors.As(chainErr, &bailErr) {
						firstErr = chainErr
					} else {
						firstErr = fmt.Errorf("item %d: %w", itemIndex, chainErr)
					}
					cancel()
				}
				mu.Unlock()
				return
			}

			// Merge child's store results back atomically — only non-internal keys
			if result != nil && result.storeData != nil {
				merged, mergeErr := deserializeStoreData(result.storeData)
				if mergeErr == nil {
					filtered := make(map[string]any)
					for k, v := range merged {
						if !strings.HasPrefix(k, "__") {
							filtered[k] = v
						}
					}
					if len(filtered) > 0 {
						wf.state.SetBatch(filtered)
					}
				}
			}

			// Flush child state to persistence before marking item completed.
			// Without this, a crash after UpdateStepStatus but before the parent
			// step's flush permanently loses the child's state contributions.
			if flushErr := wf.state.Flush(ctx); flushErr != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("flush child state for item %d: %w", i, flushErr)
					cancel()
				}
				mu.Unlock()
				return
			}

			// Track per-item completion for resume support
			if persistErr := e.persistence.UpdateStepStatus(ctx, runID, itemStepKey, Completed); persistErr != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("persist spawn item completed: %w", persistErr)
					cancel()
				}
				mu.Unlock()
			}
		}(job, i, itemKey)
	}

	wg.Wait()
	return firstErr
}

// spawnChild spawns a child process, waits for it to complete, and returns its result.
func spawnChild(ctx context.Context, port int, job *SpawnJob, secret string) (*spawnResult, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("get executable: %w", err)
	}

	cmd := exec.CommandContext(ctx, executable,
		"--gostage-child",
		fmt.Sprintf("--grpc-addr=localhost:%d", port),
		fmt.Sprintf("--job-id=%s", job.ID),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = buildChildEnv(secret, job.token)

	// Orphan detection: lifeline pipe — child inherits read end as extra fd
	lifelineR, lifelineW, pipeErr := os.Pipe()
	if pipeErr == nil {
		cmd.ExtraFiles = []*os.File{lifelineR} // fd 3 in child
		defer lifelineW.Close()                 // closed when parent dies or spawn completes
		defer lifelineR.Close()                 // parent's copy of read end
	}

	// Orphan detection: pdeathsig on Linux
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	setPdeathsig(cmd.SysProcAttr)

	startErr := cmd.Start()
	runtime.UnlockOSThread() // release thread lock set by setPdeathsig on Linux; no-op on other platforms
	if startErr != nil {
		return nil, fmt.Errorf("start child: %w", startErr)
	}

	// Wait for result from gRPC handler OR process exit
	exitCh := make(chan error, 1)

	go func() {
		exitCh <- cmd.Wait()
	}()

	select {
	case result := <-job.resultCh:
		// Got result via gRPC before process exited — wait for clean exit
		select {
		case <-exitCh:
		case <-time.After(5 * time.Second):
			// Process didn't exit cleanly — kill it
			cmd.Process.Kill()
		}
		return result, nil

	case exitErr := <-exitCh:
		// Process exited — check if we got a result
		select {
		case result := <-job.resultCh:
			return result, nil
		default:
		}
		// No result received — process crashed or errored
		if exitErr != nil {
			return nil, fmt.Errorf("child process exited: %w", exitErr)
		}
		return nil, fmt.Errorf("child process exited without sending results")

	case <-ctx.Done():
		// Context cancelled — process will be killed by CommandContext
		select {
		case <-exitCh:
		case <-time.After(5 * time.Second):
			cmd.Process.Kill()
		}
		return nil, ctx.Err()
	}
}

// defaultMaxSpawnDepth is the maximum allowed spawn chain depth.
// A depth of 1 means the root process can spawn children; children cannot spawn
// further. Increase via GOSTAGE_MAX_SPAWN_DEPTH environment variable.
const defaultMaxSpawnDepth = 5

// buildChildEnv constructs a minimal environment for child processes,
// forwarding only necessary variables and filtering out credentials.
// It also tracks the spawn chain depth via GOSTAGE_SPAWN_DEPTH.
func buildChildEnv(secret, jobToken string) []string {
	allowed := map[string]bool{
		"PATH": true, "HOME": true, "USER": true, "LOGNAME": true,
		"TMPDIR": true, "TMP": true, "TEMP": true,
		"LANG": true, "SHELL": true,
		"GOSTAGE_CHILD_TIMEOUT":   true,
		"GOSTAGE_MAX_SPAWN_DEPTH": true,
	}

	var env []string
	for _, entry := range os.Environ() {
		key, _, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		if allowed[key] || strings.HasPrefix(key, "LC_") {
			env = append(env, entry)
		}
	}

	// Increment the spawn depth counter so child processes know how deep they are.
	currentDepth := 0
	if val := os.Getenv("GOSTAGE_SPAWN_DEPTH"); val != "" {
		if d, parseErr := strconv.Atoi(val); parseErr == nil {
			currentDepth = d
		}
	}
	env = append(env, "GOSTAGE_SPAWN_DEPTH="+strconv.Itoa(currentDepth+1))

	env = append(env, "GOSTAGE_SECRET="+secret)
	env = append(env, "GOSTAGE_JOB_TOKEN="+jobToken)
	return env
}

// --- Store serialization helpers ---

// serializeStateForChild exports the workflow state plus ForEach item/index
// as a map of JSON-encoded typedEntry values for gRPC transfer.
// Each entry carries the Go type name so the child can restore original types.
func serializeStateForChild(s *runState, item any, index int) (map[string][]byte, error) {
	result, err := s.SerializeAll()
	if err != nil {
		return nil, err
	}

	// Add ForEach item/index with type metadata
	for k, v := range map[string]any{"__foreach_item": item, "__foreach_index": index} {
		jsonVal, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("marshal key %q: %w", k, err)
		}
		te := typedEntry{V: jsonVal, T: goTypeName(v)}
		data, err := json.Marshal(te)
		if err != nil {
			return nil, fmt.Errorf("marshal entry %q: %w", k, err)
		}
		result[k] = data
	}

	return result, nil
}

// deserializeStoreData converts a map of JSON-encoded typedEntry values back
// to Go values with type restoration. Each entry is expected to be a
// typedEntry{V, T} where T is the original Go type name.
func deserializeStoreData(data map[string][]byte) (map[string]any, error) {
	result := make(map[string]any, len(data))
	for k, raw := range data {
		var te typedEntry
		if err := json.Unmarshal(raw, &te); err == nil && len(te.V) > 0 {
			// Typed format: decode value and restore original type
			var val any
			if err := json.Unmarshal(te.V, &val); err != nil {
				return nil, fmt.Errorf("unmarshal value for key %q: %w", k, err)
			}
			result[k] = convertType(val, te.T)
			continue
		}
		// Fallback: untyped format (backwards compatibility)
		var val any
		if err := json.Unmarshal(raw, &val); err != nil {
			return nil, fmt.Errorf("unmarshal key %q: %w", k, err)
		}
		result[k] = val
	}
	return result, nil
}
