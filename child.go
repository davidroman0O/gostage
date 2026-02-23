package gostage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	pb "github.com/davidroman0O/gostage/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
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
//
//	func main() {
//	    if gostage.IsChild() {
//	        registerChildTasks()
//	        gostage.HandleChild()
//	        return
//	    }
//	    // parent code...
//	}
func HandleChild() {
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

	// Request our work assignment
	wfDef, err := client.RequestWorkflowDefinition(ctx, &pb.ReadySignal{ChildId: jobID})
	if err != nil {
		fmt.Fprintf(os.Stderr, "gostage child: request workflow: %v\n", err)
		os.Exit(1)
	}

	taskName := wfDef.Name

	// Deserialize store data
	storeVals, err := deserializeStoreData(wfDef.InitialStore)
	if err != nil {
		sendError(ctx, client, jobID, fmt.Sprintf("deserialize store: %v", err))
		os.Exit(1)
	}

	// Create child state (no persistence — children are memory-only)
	childState := newRunState(RunID(jobID), nil)
	for k, v := range storeVals {
		childState.SetClean(k, v) // parent data is NOT dirty
	}

	// Wire sendFn for IPC
	ipcSendFn := func(msgType string, payload any) error {
		payloadBytes, marshalErr := json.Marshal(payload)
		if marshalErr != nil {
			return marshalErr
		}
		_, rpcErr := client.SendMessage(ctx, &pb.IPCMessage{
			Type: pb.MessageType_MESSAGE_TYPE_STORE_PUT,
			Payload: &pb.IPCMessage_StorePut{
				StorePut: &pb.StorePutPayload{
					Key:       msgType,
					Value:     payloadBytes,
					ValueType: "__ipc__",
				},
			},
			Context: &pb.MessageContext{SessionId: jobID},
		})
		return rpcErr
	}

	var taskErr error

	if len(wfDef.DefinitionJson) > 0 {
		// Multi-stage workflow: rebuild from definition and execute all stages
		def, defErr := UnmarshalWorkflowDefinition(wfDef.DefinitionJson)
		if defErr != nil {
			sendError(ctx, client, jobID, fmt.Sprintf("unmarshal workflow definition: %v", defErr))
			os.Exit(1)
		}

		childWf, wfErr := NewWorkflowFromDef(def)
		if wfErr != nil {
			sendError(ctx, client, jobID, fmt.Sprintf("rebuild workflow: %v", wfErr))
			os.Exit(1)
		}

		// Share state with rebuilt workflow
		childWf.state = childState

		// Create a minimal engine for child execution (no persistence, no scheduler)
		childEngine, engErr := New()
		if engErr != nil {
			sendError(ctx, client, jobID, fmt.Sprintf("create child engine: %v", engErr))
			os.Exit(1)
		}
		defer childEngine.Close()

		// Execute the workflow (non-resuming)
		taskErr = childEngine.executeWorkflow(ctx, childWf, RunID(jobID), false)
	} else {
		// Single task execution (original behavior)
		td := lookupTask(taskName)
		if td == nil {
			sendError(ctx, client, jobID, fmt.Sprintf("task %q not registered in child", taskName))
			os.Exit(1)
		}

		taskCtx := newCtx(ctx, childState, NewDefaultLogger())
		taskCtx.sendFn = ipcSendFn

		// Set ForEach item/index on Ctx from store
		if item, ok := storeVals["__foreach_item"]; ok {
			taskCtx.forEachItem = item
		}
		if idx, ok := storeVals["__foreach_index"]; ok {
			if idxFloat, ok := idx.(float64); ok {
				taskCtx.forEachIndex = int(idxFloat)
			}
		}

		taskErr = td.fn(taskCtx)
	}

	if taskErr != nil {
		sendError(ctx, client, jobID, taskErr.Error())
		os.Exit(1)
	}

	// Send final store back to parent — only dirty keys (what the child wrote)
	finalStore := childState.ExportDirty()
	finalData := make(map[string][]byte, len(finalStore))
	for k, v := range finalStore {
		data, marshalErr := json.Marshal(v)
		if marshalErr != nil {
			continue
		}
		finalData[k] = data
	}

	client.SendMessage(ctx, &pb.IPCMessage{
		Type: pb.MessageType_MESSAGE_TYPE_FINAL_STORE,
		Payload: &pb.IPCMessage_FinalStore{
			FinalStore: &pb.FinalStorePayload{
				StoreData: finalData,
			},
		},
		Context: &pb.MessageContext{SessionId: jobID},
	})

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

// connectToParent establishes a gRPC connection to the parent server with retries and keepalive.
func connectToParent(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	var conn *grpc.ClientConn
	var err error

	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(100 * time.Millisecond * time.Duration(attempt)):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		conn, err = grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                10 * time.Second,
				Timeout:             5 * time.Second,
				PermitWithoutStream: true,
			}),
		)
		if err == nil {
			return conn, nil
		}
	}
	return nil, fmt.Errorf("connect after 3 attempts: %w", err)
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
