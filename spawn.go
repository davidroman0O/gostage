package gostage

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	pb "github.com/davidroman0O/gostage/proto"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// spawnServer manages the gRPC server and job dispatch for spawned ForEach.
// The parent process runs this server; child processes connect as clients.
type spawnServer struct {
	pb.UnimplementedWorkflowIPCServer

	mu   sync.Mutex
	jobs map[string]*spawnJob // job ID → job

	listener net.Listener
	srv      *grpc.Server
	port     int

	// engine reference for routing child IPC messages to handlers
	engine *Engine
}

// spawnJob represents one ForEach item to execute in a child process.
type spawnJob struct {
	id             string
	taskName       string
	storeData      map[string][]byte // serialized store snapshot including item/index
	definitionJSON []byte            // serialized SubWorkflowDef for multi-stage children
	resultCh       chan *spawnResult  // child sends result here (buffered, size 1)
}

// spawnResult holds the outcome of a child process execution.
type spawnResult struct {
	storeData map[string][]byte // final store from child
	err       string            // error message, empty on success
}

// newSpawnServer creates and starts a gRPC server on a random available port.
func newSpawnServer(engine *Engine) (*spawnServer, error) {
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}

	ss := &spawnServer{
		jobs:     make(map[string]*spawnJob),
		listener: lis,
		srv: grpc.NewServer(
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
func (ss *spawnServer) addJob(job *spawnJob) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.jobs[job.id] = job
}

// RequestWorkflowDefinition is called by child processes to get their work assignment.
func (ss *spawnServer) RequestWorkflowDefinition(_ context.Context, req *pb.ReadySignal) (*pb.WorkflowDefinition, error) {
	ss.mu.Lock()
	job, ok := ss.jobs[req.ChildId]
	ss.mu.Unlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "job %q not found", req.ChildId)
	}

	return &pb.WorkflowDefinition{
		Id:             job.id,
		Name:           job.taskName,
		InitialStore:   job.storeData,
		DefinitionJson: job.definitionJSON,
	}, nil
}

// SendMessage handles messages from child processes.
// Used for final store delivery and IPC messages.
func (ss *spawnServer) SendMessage(_ context.Context, msg *pb.IPCMessage) (*pb.MessageAck, error) {
	jobID := ""
	if msg.Context != nil {
		jobID = msg.Context.SessionId
	}

	switch msg.Type {
	case pb.MessageType_MESSAGE_TYPE_FINAL_STORE:
		if payload := msg.GetFinalStore(); payload != nil {
			ss.mu.Lock()
			job, ok := ss.jobs[jobID]
			ss.mu.Unlock()
			if ok {
				job.resultCh <- &spawnResult{storeData: payload.StoreData}
			}
		}

	case pb.MessageType_MESSAGE_TYPE_WORKFLOW_RESULT:
		if payload := msg.GetWorkflowResult(); payload != nil {
			if !payload.Success {
				ss.mu.Lock()
				job, ok := ss.jobs[jobID]
				ss.mu.Unlock()
				if ok {
					job.resultCh <- &spawnResult{err: payload.ErrorMessage}
				}
			}
		}

	case pb.MessageType_MESSAGE_TYPE_STORE_PUT:
		// IPC messages from child's Send() — route to engine handlers
		if payload := msg.GetStorePut(); payload != nil && payload.ValueType == "__ipc__" {
			if ss.engine != nil {
				var parsed any
				if err := json.Unmarshal(payload.Value, &parsed); err == nil {
					payloadMap, ok := parsed.(map[string]any)
					if !ok {
						payloadMap = map[string]any{"data": parsed}
					}
					ss.engine.dispatchMessage(payload.Key, payloadMap)
				}
			}
		}

	default:
		// Unknown message type — ignore
	}

	return &pb.MessageAck{Success: true, MessageId: msg.MessageId}, nil
}

// executeForEachSpawn runs ForEach items in isolated child processes.
func (e *Engine) executeForEachSpawn(ctx context.Context, wf *Workflow, s *step, items []any, runID RunID, resuming bool) error {
	ss, err := newSpawnServer(e)
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
		job := &spawnJob{
			id:        jobID,
			taskName:  s.forEachRef.taskName,
			storeData: storeData,
			resultCh:  make(chan *spawnResult, 1),
		}

		// For sub-workflow refs, serialize the workflow definition for child transfer
		if s.forEachRef.subWorkflow != nil {
			def := WorkflowToDefinition(s.forEachRef.subWorkflow)
			defJSON, defErr := MarshalWorkflowDefinition(def)
			if defErr == nil {
				job.definitionJSON = defJSON
				job.taskName = s.forEachRef.subWorkflow.ID
			}
		}
		ss.addJob(job)

		sem <- struct{}{} // acquire semaphore
		wg.Add(1)

		go func(job *spawnJob, itemIndex int, itemStepKey string) {
			defer wg.Done()
			defer func() { <-sem }() // release semaphore

			// Execute spawn with middleware chain
			var result *spawnResult
			var spawnErr error

			doSpawn := func() error {
				result, spawnErr = spawnChild(childCtx, ss.port, job)
				if spawnErr != nil {
					return spawnErr
				}
				if result.err != "" {
					return fmt.Errorf("%s", result.err)
				}
				return nil
			}

			var chainErr error
			if len(e.spawnMiddleware) > 0 {
				chain := doSpawn
				for j := len(e.spawnMiddleware) - 1; j >= 0; j-- {
					mw := e.spawnMiddleware[j]
					next := chain
					chain = func() error {
						return mw(childCtx, job, next)
					}
				}
				chainErr = chain()
			} else {
				chainErr = doSpawn()
			}

			if chainErr != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("item %d: %w", itemIndex, chainErr)
					cancel()
				}
				mu.Unlock()
				return
			}

			// Merge child's store results back — only non-internal keys
			if result != nil && result.storeData != nil {
				merged, mergeErr := deserializeStoreData(result.storeData)
				if mergeErr == nil {
					for k, v := range merged {
						if len(k) > 0 && k[0] != '_' {
							wf.state.Set(k, v)
						}
					}
				}
			}

			// Track per-item completion for resume support
			e.persistence.UpdateStepStatus(ctx, runID, itemStepKey, Completed)
		}(job, i, itemKey)
	}

	wg.Wait()
	return firstErr
}

// spawnChild spawns a child process, waits for it to complete, and returns its result.
func spawnChild(ctx context.Context, port int, job *spawnJob) (*spawnResult, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("get executable: %w", err)
	}

	cmd := exec.CommandContext(ctx, executable,
		"--gostage-child",
		fmt.Sprintf("--grpc-addr=localhost:%d", port),
		fmt.Sprintf("--job-id=%s", job.id),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

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

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start child: %w", err)
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

// --- Store serialization helpers ---

// serializeStateForChild exports the workflow state plus ForEach item/index
// as a map of JSON-encoded byte slices for gRPC transfer.
func serializeStateForChild(s *runState, item any, index int) (map[string][]byte, error) {
	export := s.ExportAll()
	export["__foreach_item"] = item
	export["__foreach_index"] = index

	result := make(map[string][]byte, len(export))
	for k, v := range export {
		data, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("marshal key %q: %w", k, err)
		}
		result[k] = data
	}
	return result, nil
}

// deserializeStoreData converts a map of JSON-encoded byte slices back to Go values.
func deserializeStoreData(data map[string][]byte) (map[string]any, error) {
	result := make(map[string]any, len(data))
	for k, v := range data {
		var val any
		if err := json.Unmarshal(v, &val); err != nil {
			return nil, fmt.Errorf("unmarshal key %q: %w", k, err)
		}
		result[k] = val
	}
	return result, nil
}
