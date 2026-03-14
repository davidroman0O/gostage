package spawn

import (
	"context"
	"testing"

	gostage "github.com/davidroman0O/gostage"
	pb "github.com/davidroman0O/gostage/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TestPerJobTokenRejection verifies the spawn server rejects messages with invalid or missing tokens.
func TestPerJobTokenRejection(t *testing.T) {
	// Create a spawnServer directly (no gRPC transport needed -- call methods in-process)
	ss := &spawnServer{
		jobs:   make(map[string]*gostage.SpawnJob),
		secret: "test-secret",
	}

	// Register a job with a known token
	ss.addJob(gostage.NewSpawnJob("job-1", "", "test-task", nil, "valid-token-123"))

	// Helper to create context with gRPC metadata
	ctxWithToken := func(jobToken string) context.Context {
		md := metadata.Pairs("x-gostage-job-token", jobToken)
		return metadata.NewIncomingContext(context.Background(), md)
	}
	ctxNoToken := func() context.Context {
		return metadata.NewIncomingContext(context.Background(), metadata.MD{})
	}

	// Test 1: Empty jobID -> InvalidArgument
	msg := &pb.IPCMessage{
		Type:    pb.MessageType_MESSAGE_TYPE_UNSPECIFIED,
		Context: &pb.MessageContext{SessionId: ""},
	}
	_, err := ss.SendMessage(ctxWithToken("valid-token-123"), msg)
	if err == nil {
		t.Fatal("expected error for empty jobID")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got: %v", err)
	}

	// Test 2: Valid jobID + wrong token -> PermissionDenied
	msg.Context.SessionId = "job-1"
	_, err = ss.SendMessage(ctxWithToken("wrong-token"), msg)
	if err == nil {
		t.Fatal("expected error for wrong token")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got: %v", err)
	}

	// Test 3: Valid jobID + no token -> PermissionDenied
	_, err = ss.SendMessage(ctxNoToken(), msg)
	if err == nil {
		t.Fatal("expected error for missing token")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got: %v", err)
	}

	// Test 4: Valid jobID + valid token -> success
	_, err = ss.SendMessage(ctxWithToken("valid-token-123"), msg)
	if err != nil {
		t.Fatalf("expected success with valid token, got: %v", err)
	}

	// Test 5: RequestWorkflowDefinition with wrong token -> PermissionDenied
	req := &pb.ReadySignal{ChildId: "job-1"}
	_, err = ss.RequestWorkflowDefinition(ctxWithToken("wrong-token"), req)
	if err == nil {
		t.Fatal("expected error for wrong token on RequestWorkflowDefinition")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got: %v", err)
	}

	// Test 6: RequestWorkflowDefinition with valid token -> success
	_, err = ss.RequestWorkflowDefinition(ctxWithToken("valid-token-123"), req)
	if err != nil {
		t.Fatalf("expected success with valid token, got: %v", err)
	}
}

func TestNewSpawnServer_StartStop(t *testing.T) {
	ss, err := newSpawnServer(nil, "test-secret")
	if err != nil {
		t.Fatal(err)
	}
	defer ss.stop()

	if ss.port == 0 {
		t.Fatal("expected non-zero port")
	}
}

func TestBuildChildEnv_DepthIncrement(t *testing.T) {
	// BuildChildEnv should increment GOSTAGE_SPAWN_DEPTH by 1.
	t.Setenv("GOSTAGE_SPAWN_DEPTH", "2")
	env := BuildChildEnv("secret", "token")

	var foundDepth string
	for _, entry := range env {
		const prefix = "GOSTAGE_SPAWN_DEPTH="
		if len(entry) > len(prefix) && entry[:len(prefix)] == prefix {
			foundDepth = entry[len(prefix):]
			break
		}
	}
	if foundDepth != "3" {
		t.Fatalf("expected GOSTAGE_SPAWN_DEPTH=3, got %q", foundDepth)
	}
}
