package spawn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	gostage "github.com/davidroman0O/gostage"
	pb "github.com/davidroman0O/gostage/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

// IsChild returns true if the current process was spawned by gostage
// as a child worker process. Checks for the --gostage-child CLI flag.
func IsChild() bool {
	for _, arg := range os.Args[1:] {
		if arg == "--gostage-child" {
			return true
		}
	}
	return false
}

// HandleChild runs the child process lifecycle:
// connect to parent gRPC server, receive task, execute, return results, exit.
//
// This function calls os.Exit and never returns.
// Tasks must be registered before calling HandleChild.
// opts are passed to the child engine (e.g. WithLogger, WithTaskMiddleware).
// Note: children always use in-memory persistence by default unless WithSQLite
// or WithPersistence is explicitly passed.
//
//	func main() {
//	    if spawn.IsChild() {
//	        registerChildTasks()
//	        spawn.HandleChild(gostage.WithLogger(myLogger))
//	        return
//	    }
//	    // parent code...
//	}
func HandleChild(opts ...gostage.EngineOption) {
	// Enforce spawn chain depth limit before doing any work.
	maxDepth := DefaultMaxSpawnDepth
	if val := os.Getenv("GOSTAGE_MAX_SPAWN_DEPTH"); val != "" {
		if d, parseErr := strconv.Atoi(val); parseErr == nil {
			maxDepth = d
		}
	}
	currentDepth := 0
	if val := os.Getenv("GOSTAGE_SPAWN_DEPTH"); val != "" {
		if d, parseErr := strconv.Atoi(val); parseErr == nil {
			currentDepth = d
		}
	}
	if currentDepth > maxDepth {
		fmt.Fprintf(os.Stderr, "gostage child: spawn depth %d exceeds maximum %d\n", currentDepth, maxDepth)
		os.Exit(1)
	}

	grpcAddr, jobID := parseChildArgs()
	if grpcAddr == "" || jobID == "" {
		fmt.Fprintf(os.Stderr, "gostage child: missing --grpc-addr or --job-id\n")
		os.Exit(1)
	}

	timeout := 5 * time.Minute
	if env := os.Getenv("GOSTAGE_CHILD_TIMEOUT"); env != "" {
		if d, err := time.ParseDuration(env); err == nil && d > 0 {
			timeout = d
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Orphan detection: open lifeline pipe (fd 3, inherited from parent)
	var lifelineFd *os.File
	if f := os.NewFile(3, "lifeline"); f != nil {
		lifelineFd = f
	}

	// Start orphan watcher (lifeline + PID polling)
	stopOrphan := startOrphanWatcher(ctx, cancel, lifelineFd)
	defer stopOrphan()

	// Connect to parent's gRPC server with retries and keepalive
	conn, err := connectToParent(ctx, grpcAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gostage child: connect to parent: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewWorkflowIPCClient(conn)

	// Attach shared secret and per-job token as gRPC metadata for all calls
	secret := os.Getenv("GOSTAGE_SECRET")
	if secret != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-gostage-secret", secret)
	}
	jobToken := os.Getenv("GOSTAGE_JOB_TOKEN")
	if jobToken != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-gostage-job-token", jobToken)
	}

	// Request our work assignment with a 10-second deadline.
	rpcCtx, rpcCancel := context.WithTimeout(ctx, 10*time.Second)
	wfDef, err := client.RequestWorkflowDefinition(rpcCtx, &pb.ReadySignal{ChildId: jobID})
	rpcCancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "gostage child: request workflow: %v\n", err)
		os.Exit(1)
	}

	taskName := wfDef.Name

	// Deserialize store data
	storeVals, err := gostage.DeserializeStoreData(wfDef.InitialStore)
	if err != nil {
		sendError(ctx, client, jobID, fmt.Sprintf("deserialize store: %v", err))
		os.Exit(1)
	}

	// Create child state (no persistence -- children are memory-only)
	childState := gostage.NewRunStateForChild(gostage.RunID(jobID))
	for k, v := range storeVals {
		childState.SetClean(k, v) // parent data is NOT dirty
	}

	// Wire sendFn for IPC -- uses the dedicated MESSAGE_TYPE_IPC message type.
	ipcSendFn := func(msgType string, payload any) error {
		payloadBytes, marshalErr := json.Marshal(payload)
		if marshalErr != nil {
			return marshalErr
		}
		_, rpcErr := client.SendMessage(ctx, &pb.IPCMessage{
			Type: pb.MessageType_MESSAGE_TYPE_IPC,
			Payload: &pb.IPCMessage_IpcPayload{
				IpcPayload: &pb.IPCPayload{
					MsgType:     msgType,
					PayloadJson: payloadBytes,
				},
			},
			Context: &pb.MessageContext{SessionId: jobID},
		})
		return rpcErr
	}

	// Create a child engine -- used for both single-task and multi-stage paths.
	// WithNoPool and WithNoScheduler skip idle goroutines that child processes
	// never use. WithSpawn is included so child processes can themselves spawn
	// grandchildren (multi-level spawn). User-supplied opts come after so they
	// can override if needed.
	childOpts := append([]gostage.EngineOption{gostage.WithNoPool(), gostage.WithNoScheduler(), WithSpawn()}, opts...)
	childEngine, engErr := gostage.New(childOpts...)
	if engErr != nil {
		sendError(ctx, client, jobID, fmt.Sprintf("create child engine: %v", engErr))
		os.Exit(1)
	}
	defer childEngine.Close()

	// Wire IPC: forward Send() calls from tasks inside the child to the parent.
	childEngine.OnMessage("*", func(msgType string, payload map[string]any) {
		ipcSendFn(msgType, payload)
	})

	var taskErr error

	if len(wfDef.DefinitionJson) > 0 {
		// Multi-stage workflow: rebuild from definition and execute all stages.
		def, defErr := gostage.UnmarshalWorkflowDefinition(wfDef.DefinitionJson)
		if defErr != nil {
			sendError(ctx, client, jobID, fmt.Sprintf("unmarshal workflow definition: %v", defErr))
			os.Exit(1)
		}

		childWf, wfErr := gostage.NewWorkflowFromDef(def)
		if wfErr != nil {
			sendError(ctx, client, jobID, fmt.Sprintf("rebuild workflow: %v", wfErr))
			os.Exit(1)
		}

		// Share state with rebuilt workflow
		childWf.SetRunStateFromChild(childState)

		// Register a run record in the child engine's in-memory persistence
		// so executeWorkflow's UpdateStepStatus calls have a run to write to.
		now := time.Now()
		childRun := &gostage.RunState{
			RunID:      gostage.RunID(jobID),
			WorkflowID: childWf.ID,
			Status:     gostage.Running,
			StepStates: make(map[string]gostage.Status),
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		if saveErr := childEngine.EnginePersistence().SaveRun(ctx, childRun); saveErr != nil {
			sendError(ctx, client, jobID, fmt.Sprintf("register child run: %v", saveErr))
			os.Exit(1)
		}

		// Execute the workflow (non-resuming)
		taskErr = childEngine.ExecuteWorkflowForChild(ctx, childWf, gostage.RunID(jobID), false)
	} else {
		// Single task execution -- route through the child engine's retry and
		// middleware machinery so opts like WithTaskMiddleware apply here too.
		taskErr = childEngine.ExecuteSingleTaskForChild(ctx, taskName, childState, storeVals, ipcSendFn)
	}

	if taskErr != nil {
		// Bail signals propagate as a typed bail result so the parent can
		// end the workflow with Bailed status rather than Failed.
		var bailErr *gostage.BailError
		if errors.As(taskErr, &bailErr) {
			sendBail(ctx, client, jobID, bailErr.Reason)
			os.Exit(0)
		}
		// SuspendError in a spawned child has no supported semantics
		var suspendErr *gostage.SuspendError
		if errors.As(taskErr, &suspendErr) {
			sendError(ctx, client, jobID, "suspend is not supported in spawned child processes")
			os.Exit(1)
		}
		sendError(ctx, client, jobID, taskErr.Error())
		os.Exit(1)
	}

	// Send final store back to parent -- only dirty keys (what the child wrote)
	finalData, serErr := childState.SerializeDirty()
	if serErr != nil {
		sendError(ctx, client, jobID, fmt.Sprintf("serialize results: %v", serErr))
		os.Exit(1)
	}

	_, sendErr := client.SendMessage(ctx, &pb.IPCMessage{
		Type: pb.MessageType_MESSAGE_TYPE_FINAL_STORE,
		Payload: &pb.IPCMessage_FinalStore{
			FinalStore: &pb.FinalStorePayload{
				StoreData: finalData,
			},
		},
		Context: &pb.MessageContext{SessionId: jobID},
	})
	if sendErr != nil {
		fmt.Fprintf(os.Stderr, "gostage child: send final store: %v\n", sendErr)
		os.Exit(1)
	}

	os.Exit(0)
}

// parseChildArgs extracts --grpc-addr and --job-id from os.Args.
func parseChildArgs() (grpcAddr, jobID string) {
	for _, arg := range os.Args[1:] {
		if strings.HasPrefix(arg, "--grpc-addr=") {
			grpcAddr = strings.TrimPrefix(arg, "--grpc-addr=")
		}
		if strings.HasPrefix(arg, "--job-id=") {
			jobID = strings.TrimPrefix(arg, "--job-id=")
		}
	}
	return
}

// connectToParent establishes a gRPC connection to the parent server with keepalive.
func connectToParent(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create gRPC client: %w", err)
	}
	return conn, nil
}

// sendError sends an error result back to the parent.
func sendError(ctx context.Context, client pb.WorkflowIPCClient, jobID, errMsg string) {
	client.SendMessage(ctx, &pb.IPCMessage{
		Type: pb.MessageType_MESSAGE_TYPE_WORKFLOW_RESULT,
		Payload: &pb.IPCMessage_WorkflowResult{
			WorkflowResult: &pb.WorkflowResultPayload{
				Success:      false,
				ErrorMessage: errMsg,
			},
		},
		Context: &pb.MessageContext{SessionId: jobID},
	})
}

// sendBail sends a bail signal back to the parent so the parent workflow
// ends with Bailed status rather than Failed.
func sendBail(ctx context.Context, client pb.WorkflowIPCClient, jobID, reason string) {
	client.SendMessage(ctx, &pb.IPCMessage{
		Type: pb.MessageType_MESSAGE_TYPE_BAIL,
		Payload: &pb.IPCMessage_BailPayload{
			BailPayload: &pb.BailPayload{
				Reason: reason,
			},
		},
		Context: &pb.MessageContext{SessionId: jobID},
	})
}
