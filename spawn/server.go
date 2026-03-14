package spawn

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

	gostage "github.com/davidroman0O/gostage"
	pb "github.com/davidroman0O/gostage/proto"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Runner implements gostage.SpawnRunner using gRPC-based child processes.
type Runner struct{}

// NewRunner creates a new spawn runner.
func NewRunner() *Runner {
	return &Runner{}
}

// Close is a no-op — the Runner has no persistent resources.
// Each ExecuteForEachSpawn call starts and stops its own gRPC server.
func (r *Runner) Close() error {
	return nil
}

// spawnServer manages the gRPC server and job dispatch for spawned ForEach.
// The parent process runs this server; child processes connect as clients.
type spawnServer struct {
	pb.UnimplementedWorkflowIPCServer

	mu   sync.Mutex
	jobs map[string]*gostage.SpawnJob // job ID -> job

	listener net.Listener
	srv      *grpc.Server
	port     int

	// engine reference for routing child IPC messages to handlers
	engine *gostage.Engine

	secret string
}

// spawnResult holds the outcome of a child process execution.
type spawnResult struct {
	storeData  map[string][]byte // final store from child
	err        string            // error message, empty on success
	bailReason string            // bail reason if the child bailed, empty otherwise
}

// newSpawnServer creates and starts a gRPC server on a random available port.
func newSpawnServer(engine *gostage.Engine, secret string) (*spawnServer, error) {
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
		jobs:     make(map[string]*gostage.SpawnJob),
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
func (ss *spawnServer) addJob(job *gostage.SpawnJob) {
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
	if len(jobTokens) == 0 || subtle.ConstantTimeCompare([]byte(jobTokens[0]), []byte(job.Token())) != 1 {
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
	if len(jobTokens) == 0 || subtle.ConstantTimeCompare([]byte(jobTokens[0]), []byte(job.Token())) != 1 {
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
				case job.ResultCh() <- &gostage.SpawnResult{StoreData: payload.StoreData}:
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
					case job.ResultCh() <- &gostage.SpawnResult{Err: payload.ErrorMessage}:
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
				case job.ResultCh() <- &gostage.SpawnResult{BailReason: payload.Reason}:
				default:
				}
			}
		}

	case pb.MessageType_MESSAGE_TYPE_IPC:
		// Dedicated IPC message type -- route to engine message handlers.
		if payload := msg.GetIpcPayload(); payload != nil && ss.engine != nil {
			var parsed any
			if err := json.Unmarshal(payload.PayloadJson, &parsed); err == nil {
				payloadMap, ok := parsed.(map[string]any)
				if !ok {
					payloadMap = map[string]any{"data": parsed}
				}
				jobRunID := gostage.RunID("")
				ss.mu.Lock()
				if job, ok := ss.jobs[jobID]; ok {
					jobRunID = job.RunID
				}
				ss.mu.Unlock()
				ss.engine.DispatchMessage(payload.MsgType, payloadMap, jobRunID)
			}
		}

	default:
		// Unknown message type -- ignore
	}

	return &pb.MessageAck{Success: true, MessageId: msg.MessageId}, nil
}

// ExecuteForEachSpawn runs ForEach items in isolated child processes.
func (r *Runner) ExecuteForEachSpawn(ctx context.Context, e *gostage.Engine, wf *gostage.Workflow, cfg gostage.SpawnConfig, items []any, runID gostage.RunID, resuming bool) error {
	secret := uuid.New().String()
	ss, err := newSpawnServer(e, secret)
	if err != nil {
		return fmt.Errorf("start spawn server: %w", err)
	}
	defer ss.stop()

	// Load step states for per-item resume tracking
	var stepStates map[string]gostage.Status
	if resuming {
		run, loadErr := e.EnginePersistence().LoadRun(ctx, runID)
		if loadErr == nil && run.StepStates != nil {
			stepStates = run.StepStates
		}
	}

	concurrency := cfg.Concurrency
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
		itemKey := fmt.Sprintf("%s:%d", cfg.StepID, i)
		if resuming && stepStates != nil && stepStates[itemKey] == gostage.Completed {
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
		storeData, serErr := gostage.SerializeStateForChild(wf.RunState(), item, i)
		if serErr != nil {
			return fmt.Errorf("serialize state for item %d: %w", i, serErr)
		}

		jobID := uuid.New().String()
		jobToken := uuid.New().String()
		job := gostage.NewSpawnJob(jobID, runID, cfg.TaskName, storeData, jobToken)

		// For sub-workflow refs, serialize the workflow definition for child transfer
		if cfg.SubWorkflow != nil {
			def, defErr := gostage.WorkflowToDefinition(cfg.SubWorkflow)
			if defErr != nil {
				return fmt.Errorf("serialize workflow definition: %w", defErr)
			}
			defJSON, marshalErr := gostage.MarshalWorkflowDefinition(def)
			if marshalErr != nil {
				return fmt.Errorf("marshal workflow definition: %w", marshalErr)
			}
			job.DefinitionJSON = defJSON
			job.TaskName = cfg.SubWorkflow.ID
		}
		ss.addJob(job)

		sem <- struct{}{} // acquire semaphore
		wg.Add(1)

		go func(job *gostage.SpawnJob, itemIndex int, itemStepKey string) {
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
			var result *gostage.SpawnResult
			var spawnErr error

			doSpawn := func() error {
				result, spawnErr = spawnChild(childCtx, ss.port, job, ss.secret)
				if spawnErr != nil {
					return spawnErr
				}
				if result.BailReason != "" {
					return &gostage.BailError{Reason: result.BailReason}
				}
				if result.Err != "" {
					return fmt.Errorf("%s", result.Err)
				}
				return nil
			}

			childMW := e.ChildMiddlewareChain()
			var chainErr error
			if len(childMW) > 0 {
				chain := doSpawn
				for j := 0; j < len(childMW); j++ {
					mw := childMW[j]
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
					// through executeForEachSpawn -> executeWorkflow -> doExecute
					// with Bailed status intact.
					var bailErr *gostage.BailError
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

			// Merge child's store results back atomically -- only non-internal keys
			if result != nil && result.StoreData != nil {
				merged, mergeErr := gostage.DeserializeStoreData(result.StoreData)
				if mergeErr == nil {
					filtered := make(map[string]any)
					for k, v := range merged {
						if !strings.HasPrefix(k, "__") {
							filtered[k] = v
						}
					}
					if len(filtered) > 0 {
						wf.RunState().SetBatch(filtered)
					}
				}
			}

			// Flush child state to persistence before marking item completed.
			if flushErr := wf.RunState().Flush(childCtx); flushErr != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("flush child state for item %d: %w", itemIndex, flushErr)
					cancel()
				}
				mu.Unlock()
				return
			}

			// Track per-item completion for resume support
			if persistErr := e.EnginePersistence().UpdateStepStatus(childCtx, runID, itemStepKey, gostage.Completed); persistErr != nil {
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
func spawnChild(ctx context.Context, port int, job *gostage.SpawnJob, secret string) (*gostage.SpawnResult, error) {
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
	cmd.Env = BuildChildEnv(secret, job.Token())

	// Orphan detection: lifeline pipe -- child inherits read end as extra fd
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
	case result := <-job.ResultCh():
		// Got result via gRPC before process exited -- wait for clean exit
		select {
		case <-exitCh:
		case <-time.After(5 * time.Second):
			// Process didn't exit cleanly -- kill it
			cmd.Process.Kill()
		}
		return result, nil

	case exitErr := <-exitCh:
		// Process exited -- check if we got a result
		select {
		case result := <-job.ResultCh():
			return result, nil
		default:
		}
		// No result received -- process crashed or errored
		if exitErr != nil {
			return nil, fmt.Errorf("child process exited: %w", exitErr)
		}
		return nil, fmt.Errorf("child process exited without sending results")

	case <-ctx.Done():
		// Context cancelled -- process will be killed by CommandContext
		select {
		case <-exitCh:
		case <-time.After(5 * time.Second):
			cmd.Process.Kill()
		}
		return nil, ctx.Err()
	}
}

// DefaultMaxSpawnDepth is the maximum allowed spawn chain depth.
// A depth of 1 means the root process can spawn children; children cannot spawn
// further. Increase via GOSTAGE_MAX_SPAWN_DEPTH environment variable.
const DefaultMaxSpawnDepth = 5

// BuildChildEnv constructs a minimal environment for child processes,
// forwarding only necessary variables and filtering out credentials.
// It also tracks the spawn chain depth via GOSTAGE_SPAWN_DEPTH.
func BuildChildEnv(secret, jobToken string) []string {
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
