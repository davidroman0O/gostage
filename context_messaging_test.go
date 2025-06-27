package gostage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var contextMessageActionRegistered sync.Once

// Test action for context messaging tests
type TestContextMessageAction struct {
	BaseAction
	messagesSent  []map[string]interface{}
	customContext map[string]interface{}
	mu            sync.Mutex
}

func (a *TestContextMessageAction) Execute(ctx *ActionContext) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Send a test message with automatic context
	testPayload := map[string]interface{}{
		"test_key":    "test_value",
		"action_name": a.Name(),
	}

	err := ctx.Send(MessageTypeStorePut, testPayload)
	if err == nil {
		a.messagesSent = append(a.messagesSent, testPayload)
	}

	// Send custom context message if specified
	if a.customContext != nil {
		customPayload := map[string]interface{}{
			"custom_key": "custom_value",
		}
		err = ctx.SendWithCustomContext(MessageTypeStorePut, customPayload, a.customContext)
		if err == nil {
			a.messagesSent = append(a.messagesSent, customPayload)
		}
	}

	return err
}

func (a *TestContextMessageAction) GetMessagesSent() []map[string]interface{} {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]map[string]interface{}{}, a.messagesSent...)
}

func (a *TestContextMessageAction) SetCustomContext(ctx map[string]interface{}) {
	a.customContext = ctx
}

// Register test action only once
func registerTestContextMessageAction() {
	contextMessageActionRegistered.Do(func() {
		RegisterAction("test-context-message", func() Action {
			return &TestContextMessageAction{
				BaseAction: NewBaseAction("test-context-message", "Test action for context messaging"),
			}
		})
	})
}

func TestContextMessaging_ContextMetadata(t *testing.T) {
	registerTestContextMessageAction()

	// Create workflow with context messaging
	runner := NewRunner(WithGRPCTransport("localhost", 50090))
	defer runner.Broker.Close()

	// Track received messages with context
	var receivedMessages []struct {
		MessageType MessageType
		Payload     json.RawMessage
		Context     MessageContext
	}
	var mu sync.Mutex

	// Register context handler to capture context
	runner.Broker.RegisterHandlerWithContext(MessageTypeStorePut,
		func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
			mu.Lock()
			defer mu.Unlock()
			receivedMessages = append(receivedMessages, struct {
				MessageType MessageType
				Payload     json.RawMessage
				Context     MessageContext
			}{msgType, payload, context})
			return nil
		})

	// Create test workflow
	workflow := NewWorkflow("test-context-workflow", "Test Context Workflow", "Testing context messaging")
	stage := NewStage("test-stage", "Test Stage", "Test stage for context messaging")

	action := &TestContextMessageAction{
		BaseAction: NewBaseAction("test-action", "Test Action"),
	}
	stage.AddAction(action)
	workflow.AddStage(stage)

	// Execute workflow
	ctx := context.Background()
	err := runner.Execute(ctx, workflow, NewDefaultLogger())
	require.NoError(t, err)

	// Verify context metadata
	mu.Lock()
	defer mu.Unlock()

	require.Len(t, receivedMessages, 1, "Should have received exactly 1 message")

	msg := receivedMessages[0]
	assert.Equal(t, MessageTypeStorePut, msg.MessageType)
	assert.Equal(t, "test-context-workflow", msg.Context.WorkflowID)
	assert.Equal(t, "test-stage", msg.Context.StageID)
	assert.Equal(t, "test-action", msg.Context.ActionName)
	assert.Equal(t, int32(os.Getpid()), msg.Context.ProcessID)
	assert.False(t, msg.Context.IsChildProcess, "Should not be child process in normal execution")
	assert.Equal(t, int32(0), msg.Context.ActionIndex, "Should be first action in stage")
	assert.True(t, msg.Context.IsLastAction, "Should be last action in stage")
	assert.NotEmpty(t, msg.Context.SessionID, "Session ID should be set")
	assert.Greater(t, msg.Context.SequenceNumber, int64(0), "Sequence number should be positive")
}

func TestContextMessaging_CustomContext(t *testing.T) {
	registerTestContextMessageAction()

	runner := NewRunner(WithGRPCTransport("localhost", 50091))
	defer runner.Broker.Close()

	var receivedContext MessageContext
	var mu sync.Mutex

	runner.Broker.RegisterHandlerWithContext(MessageTypeStorePut,
		func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
			mu.Lock()
			defer mu.Unlock()
			// Only capture the second message (with custom context)
			if context.WorkflowID == "custom-workflow-override" {
				receivedContext = context
			}
			return nil
		})

	// Create workflow
	workflow := NewWorkflow("original-workflow", "Original Workflow", "Testing custom context")
	stage := NewStage("original-stage", "Original Stage", "Original stage")

	action := &TestContextMessageAction{
		BaseAction: NewBaseAction("original-action", "Original Action"),
	}
	// Set custom context to override workflow ID
	action.SetCustomContext(map[string]interface{}{
		"workflow_id": "custom-workflow-override",
	})
	stage.AddAction(action)
	workflow.AddStage(stage)

	// Execute workflow
	err := runner.Execute(context.Background(), workflow, NewDefaultLogger())
	require.NoError(t, err)

	// Verify custom context override
	mu.Lock()
	defer mu.Unlock()

	assert.Equal(t, "custom-workflow-override", receivedContext.WorkflowID, "Workflow ID should be overridden")
	assert.Equal(t, "original-stage", receivedContext.StageID, "Stage ID should remain original")
	assert.Equal(t, "original-action", receivedContext.ActionName, "Action name should remain original")
}

func TestContextMessaging_WorkflowSpecificHandler(t *testing.T) {
	registerTestContextMessageAction()

	runner := NewRunner(WithGRPCTransport("localhost", 50092))
	defer runner.Broker.Close()

	var targetWorkflowMessages []MessageContext
	var otherWorkflowMessages []MessageContext
	var mu sync.Mutex

	// Register workflow-specific handler
	runner.Broker.RegisterWorkflowHandler(MessageTypeStorePut, "target-workflow",
		func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
			mu.Lock()
			defer mu.Unlock()
			targetWorkflowMessages = append(targetWorkflowMessages, context)
			return nil
		})

	// Register global handler to catch other messages
	runner.Broker.RegisterHandlerWithContext(MessageTypeStorePut,
		func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
			mu.Lock()
			defer mu.Unlock()
			if context.WorkflowID != "target-workflow" {
				otherWorkflowMessages = append(otherWorkflowMessages, context)
			}
			return nil
		})

	// Create target workflow
	targetWorkflow := NewWorkflow("target-workflow", "Target Workflow", "Targeted workflow")
	targetStage := NewStage("target-stage", "Target Stage", "Target stage")
	targetAction := &TestContextMessageAction{
		BaseAction: NewBaseAction("target-action", "Target Action"),
	}
	targetStage.AddAction(targetAction)
	targetWorkflow.AddStage(targetStage)

	// Create other workflow
	otherWorkflow := NewWorkflow("other-workflow", "Other Workflow", "Other workflow")
	otherStage := NewStage("other-stage", "Other Stage", "Other stage")
	otherAction := &TestContextMessageAction{
		BaseAction: NewBaseAction("other-action", "Other Action"),
	}
	otherStage.AddAction(otherAction)
	otherWorkflow.AddStage(otherStage)

	// Execute both workflows
	err := runner.Execute(context.Background(), targetWorkflow, NewDefaultLogger())
	require.NoError(t, err)

	err = runner.Execute(context.Background(), otherWorkflow, NewDefaultLogger())
	require.NoError(t, err)

	// Verify targeted handling
	mu.Lock()
	defer mu.Unlock()

	assert.Len(t, targetWorkflowMessages, 1, "Should have received 1 message from target workflow")
	assert.Len(t, otherWorkflowMessages, 1, "Should have received 1 message from other workflow")

	assert.Equal(t, "target-workflow", targetWorkflowMessages[0].WorkflowID)
	assert.Equal(t, "other-workflow", otherWorkflowMessages[0].WorkflowID)
}

func TestContextMessaging_StageSpecificHandler(t *testing.T) {
	registerTestContextMessageAction()

	runner := NewRunner(WithGRPCTransport("localhost", 50093))
	defer runner.Broker.Close()

	var targetStageMessages []MessageContext
	var otherStageMessages []MessageContext
	var mu sync.Mutex

	// Register stage-specific handler
	runner.Broker.RegisterStageHandler(MessageTypeStorePut, "test-workflow", "target-stage",
		func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
			mu.Lock()
			defer mu.Unlock()
			targetStageMessages = append(targetStageMessages, context)
			return nil
		})

	// Register global handler to catch other messages
	runner.Broker.RegisterHandlerWithContext(MessageTypeStorePut,
		func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
			mu.Lock()
			defer mu.Unlock()
			if context.StageID != "target-stage" {
				otherStageMessages = append(otherStageMessages, context)
			}
			return nil
		})

	// Create workflow with multiple stages
	workflow := NewWorkflow("test-workflow", "Test Workflow", "Testing stage-specific handlers")

	// Target stage
	targetStage := NewStage("target-stage", "Target Stage", "Target stage")
	targetAction := &TestContextMessageAction{
		BaseAction: NewBaseAction("target-action", "Target Action"),
	}
	targetStage.AddAction(targetAction)
	workflow.AddStage(targetStage)

	// Other stage
	otherStage := NewStage("other-stage", "Other Stage", "Other stage")
	otherAction := &TestContextMessageAction{
		BaseAction: NewBaseAction("other-action", "Other Action"),
	}
	otherStage.AddAction(otherAction)
	workflow.AddStage(otherStage)

	// Execute workflow
	err := runner.Execute(context.Background(), workflow, NewDefaultLogger())
	require.NoError(t, err)

	// Verify stage-specific handling
	mu.Lock()
	defer mu.Unlock()

	assert.Len(t, targetStageMessages, 1, "Should have received 1 message from target stage")
	assert.Len(t, otherStageMessages, 1, "Should have received 1 message from other stage")

	assert.Equal(t, "target-stage", targetStageMessages[0].StageID)
	assert.Equal(t, "other-stage", otherStageMessages[0].StageID)
}

func TestContextMessaging_ActionSpecificHandler(t *testing.T) {
	registerTestContextMessageAction()

	runner := NewRunner(WithGRPCTransport("localhost", 50094))
	defer runner.Broker.Close()

	var targetActionMessages []MessageContext
	var otherActionMessages []MessageContext
	var mu sync.Mutex

	// Register action-specific handler
	runner.Broker.RegisterActionHandler(MessageTypeStorePut, "test-workflow", "test-stage", "target-action",
		func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
			mu.Lock()
			defer mu.Unlock()
			targetActionMessages = append(targetActionMessages, context)
			return nil
		})

	// Register global handler to catch other messages
	runner.Broker.RegisterHandlerWithContext(MessageTypeStorePut,
		func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
			mu.Lock()
			defer mu.Unlock()
			if context.ActionName != "target-action" {
				otherActionMessages = append(otherActionMessages, context)
			}
			return nil
		})

	// Create workflow with multiple actions in same stage
	workflow := NewWorkflow("test-workflow", "Test Workflow", "Testing action-specific handlers")
	stage := NewStage("test-stage", "Test Stage", "Test stage")

	// Target action
	targetAction := &TestContextMessageAction{
		BaseAction: NewBaseAction("target-action", "Target Action"),
	}
	stage.AddAction(targetAction)

	// Other action
	otherAction := &TestContextMessageAction{
		BaseAction: NewBaseAction("other-action", "Other Action"),
	}
	stage.AddAction(otherAction)

	workflow.AddStage(stage)

	// Execute workflow
	err := runner.Execute(context.Background(), workflow, NewDefaultLogger())
	require.NoError(t, err)

	// Verify action-specific handling
	mu.Lock()
	defer mu.Unlock()

	assert.Len(t, targetActionMessages, 1, "Should have received 1 message from target action")
	assert.Len(t, otherActionMessages, 1, "Should have received 1 message from other action")

	assert.Equal(t, "target-action", targetActionMessages[0].ActionName)
	assert.Equal(t, "other-action", otherActionMessages[0].ActionName)
}

func TestContextMessaging_SequenceNumbers(t *testing.T) {
	registerTestContextMessageAction()

	runner := NewRunner(WithGRPCTransport("localhost", 50095))
	defer runner.Broker.Close()

	var sequenceNumbers []int64
	var mu sync.Mutex

	runner.Broker.RegisterHandlerWithContext(MessageTypeStorePut,
		func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
			mu.Lock()
			defer mu.Unlock()
			sequenceNumbers = append(sequenceNumbers, context.SequenceNumber)
			return nil
		})

	// Create workflow with multiple actions to generate multiple messages
	workflow := NewWorkflow("sequence-test", "Sequence Test", "Testing sequence numbers")
	stage := NewStage("sequence-stage", "Sequence Stage", "Sequence stage")

	// Add multiple actions
	for i := 0; i < 3; i++ {
		action := &TestContextMessageAction{
			BaseAction: NewBaseAction(fmt.Sprintf("action-%d", i), fmt.Sprintf("Action %d", i)),
		}
		stage.AddAction(action)
	}

	workflow.AddStage(stage)

	// Execute workflow
	err := runner.Execute(context.Background(), workflow, NewDefaultLogger())
	require.NoError(t, err)

	// Verify sequence numbers are incrementing
	mu.Lock()
	defer mu.Unlock()

	require.Len(t, sequenceNumbers, 3, "Should have received 3 messages")

	for i := 1; i < len(sequenceNumbers); i++ {
		assert.Greater(t, sequenceNumbers[i], sequenceNumbers[i-1],
			"Sequence numbers should be incrementing")
	}
}

func TestContextMessaging_SessionID(t *testing.T) {
	registerTestContextMessageAction()

	runner1 := NewRunner(WithGRPCTransport("localhost", 50096))
	defer runner1.Broker.Close()

	runner2 := NewRunner(WithGRPCTransport("localhost", 50097))
	defer runner2.Broker.Close()

	var sessionIDs []string
	var mu sync.Mutex

	handler := func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
		mu.Lock()
		defer mu.Unlock()
		sessionIDs = append(sessionIDs, context.SessionID)
		return nil
	}

	runner1.Broker.RegisterHandlerWithContext(MessageTypeStorePut, handler)
	runner2.Broker.RegisterHandlerWithContext(MessageTypeStorePut, handler)

	// Create identical workflows for both runners
	createWorkflow := func() *Workflow {
		workflow := NewWorkflow("session-test", "Session Test", "Testing session IDs")
		stage := NewStage("session-stage", "Session Stage", "Session stage")
		action := &TestContextMessageAction{
			BaseAction: NewBaseAction("session-action", "Session Action"),
		}
		stage.AddAction(action)
		workflow.AddStage(stage)
		return workflow
	}

	// Execute workflows on different runners
	err := runner1.Execute(context.Background(), createWorkflow(), NewDefaultLogger())
	require.NoError(t, err)

	err = runner2.Execute(context.Background(), createWorkflow(), NewDefaultLogger())
	require.NoError(t, err)

	// Verify different session IDs
	mu.Lock()
	defer mu.Unlock()

	require.Len(t, sessionIDs, 2, "Should have received 2 messages")
	assert.NotEqual(t, sessionIDs[0], sessionIDs[1], "Different runners should have different session IDs")

	for _, sessionID := range sessionIDs {
		assert.NotEmpty(t, sessionID, "Session ID should not be empty")
		assert.Contains(t, sessionID, "session-", "Session ID should have expected format")
	}
}

func TestContextMessaging_ActionPosition(t *testing.T) {
	registerTestContextMessageAction()

	runner := NewRunner(WithGRPCTransport("localhost", 50098))
	defer runner.Broker.Close()

	var actionPositions []struct {
		ActionName string
		Index      int32
		IsLast     bool
	}
	var mu sync.Mutex

	runner.Broker.RegisterHandlerWithContext(MessageTypeStorePut,
		func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
			mu.Lock()
			defer mu.Unlock()
			actionPositions = append(actionPositions, struct {
				ActionName string
				Index      int32
				IsLast     bool
			}{
				ActionName: context.ActionName,
				Index:      context.ActionIndex,
				IsLast:     context.IsLastAction,
			})
			return nil
		})

	// Create workflow with multiple actions
	workflow := NewWorkflow("position-test", "Position Test", "Testing action positions")
	stage := NewStage("position-stage", "Position Stage", "Position stage")

	actionNames := []string{"first-action", "middle-action", "last-action"}
	for _, name := range actionNames {
		action := &TestContextMessageAction{
			BaseAction: NewBaseAction(name, name),
		}
		stage.AddAction(action)
	}

	workflow.AddStage(stage)

	// Execute workflow
	err := runner.Execute(context.Background(), workflow, NewDefaultLogger())
	require.NoError(t, err)

	// Verify action positions
	mu.Lock()
	defer mu.Unlock()

	require.Len(t, actionPositions, 3, "Should have received 3 messages")

	for i, pos := range actionPositions {
		assert.Equal(t, actionNames[i], pos.ActionName, "Action name should match expected order")
		assert.Equal(t, int32(i), pos.Index, "Action index should match position")

		if i == len(actionPositions)-1 {
			assert.True(t, pos.IsLast, "Last action should be marked as last")
		} else {
			assert.False(t, pos.IsLast, "Non-last actions should not be marked as last")
		}
	}
}

func TestContextMessaging_BackwardCompatibility(t *testing.T) {
	registerTestContextMessageAction()

	runner := NewRunner(WithGRPCTransport("localhost", 50099))
	defer runner.Broker.Close()

	var legacyMessages []MessageType
	var contextMessages []MessageContext
	var mu sync.Mutex

	// Register legacy handler (should still work)
	runner.Broker.RegisterHandler(MessageTypeStorePut, func(msgType MessageType, payload json.RawMessage) error {
		mu.Lock()
		defer mu.Unlock()
		legacyMessages = append(legacyMessages, msgType)
		return nil
	})

	// Register context handler (should also work)
	runner.Broker.RegisterHandlerWithContext(MessageTypeStorePut,
		func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
			mu.Lock()
			defer mu.Unlock()
			contextMessages = append(contextMessages, context)
			return nil
		})

	// Create and execute workflow
	workflow := NewWorkflow("compat-test", "Compatibility Test", "Testing backward compatibility")
	stage := NewStage("compat-stage", "Compatibility Stage", "Compatibility stage")
	action := &TestContextMessageAction{
		BaseAction: NewBaseAction("compat-action", "Compatibility Action"),
	}
	stage.AddAction(action)
	workflow.AddStage(stage)

	err := runner.Execute(context.Background(), workflow, NewDefaultLogger())
	require.NoError(t, err)

	// Verify both handlers received messages
	mu.Lock()
	defer mu.Unlock()

	assert.Len(t, legacyMessages, 1, "Legacy handler should receive message")
	assert.Len(t, contextMessages, 1, "Context handler should receive message")

	assert.Equal(t, MessageTypeStorePut, legacyMessages[0])
	assert.Equal(t, "compat-test", contextMessages[0].WorkflowID)
}

// Benchmark context messaging overhead
func BenchmarkContextMessaging_ContextOverhead(b *testing.B) {
	registerTestContextMessageAction()

	runner := NewRunner(WithGRPCTransport("localhost", 50100))
	defer runner.Broker.Close()

	// Simple handler that does minimal work
	runner.Broker.RegisterHandlerWithContext(MessageTypeStorePut,
		func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
			return nil
		})

	// Create simple workflow
	workflow := NewWorkflow("bench-test", "Benchmark Test", "Benchmark test")
	stage := NewStage("bench-stage", "Benchmark Stage", "Benchmark stage")
	action := &TestContextMessageAction{
		BaseAction: NewBaseAction("bench-action", "Benchmark Action"),
	}
	stage.AddAction(action)
	workflow.AddStage(stage)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := runner.Execute(context.Background(), workflow, NewDefaultLogger())
		if err != nil {
			b.Fatal(err)
		}
	}
}

// TestContextMessaging_NormalWorkflowWithoutGRPC tests that context messaging works
// for normal (non-spawned) workflows without requiring gRPC setup
func TestContextMessaging_NormalWorkflowWithoutGRPC(t *testing.T) {
	// Create a regular runner (no gRPC transport)
	runner := NewRunner()

	var receivedMessages []struct {
		MessageType MessageType
		Context     MessageContext
		Payload     map[string]interface{}
	}

	// Register context handler for normal workflows
	runner.Broker.RegisterHandlerWithContext(MessageTypeStorePut,
		func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
			var data map[string]interface{}
			json.Unmarshal(payload, &data)

			receivedMessages = append(receivedMessages, struct {
				MessageType MessageType
				Context     MessageContext
				Payload     map[string]interface{}
			}{
				MessageType: msgType,
				Context:     context,
				Payload:     data,
			})

			return nil
		})

	// Create a normal workflow (not for spawning)
	workflow := NewWorkflow("normal-workflow", "Normal Workflow Test", "1.0.0")

	stage1 := NewStage("stage-1", "Stage One", "First stage")
	workflow.AddStage(stage1)

	// Create a test action that sends messages
	testAction := &TestContextMessageAction{
		BaseAction: NewBaseAction("test-action", "Test action for context messaging"),
	}
	stage1.AddAction(testAction)

	// Execute the normal workflow
	ctx := context.Background()
	logger := &TestLogger{t: t}

	err := runner.Execute(ctx, workflow, logger)
	assert.NoError(t, err)

	// Verify we received the message with proper context
	assert.Len(t, receivedMessages, 1)

	msg := receivedMessages[0]
	assert.Equal(t, MessageTypeStorePut, msg.MessageType)
	assert.Equal(t, "normal-workflow", msg.Context.WorkflowID)
	assert.Equal(t, "stage-1", msg.Context.StageID)
	assert.Equal(t, "test-action", msg.Context.ActionName)
	assert.Equal(t, int32(os.Getpid()), msg.Context.ProcessID)
	assert.False(t, msg.Context.IsChildProcess)        // Normal workflow = not child process
	assert.Equal(t, int32(0), msg.Context.ActionIndex) // First action in stage
	assert.True(t, msg.Context.IsLastAction)           // Only action in stage
	assert.NotEmpty(t, msg.Context.SessionID)
	assert.Equal(t, int64(1), msg.Context.SequenceNumber)

	// Verify payload
	assert.Equal(t, "test_value", msg.Payload["test_key"])
	assert.Equal(t, "test-action", msg.Payload["action_name"])
}

// TestContextMessaging_WorkflowSpecificHandlerNormalWorkflow tests workflow-specific
// handlers work with normal workflows
func TestContextMessaging_WorkflowSpecificHandlerNormalWorkflow(t *testing.T) {
	runner := NewRunner()

	var globalMessages []MessageContext
	var workflowMessages []MessageContext
	var otherWorkflowMessages []MessageContext

	// Global handler
	runner.Broker.RegisterHandlerWithContext(MessageTypeStorePut,
		func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
			globalMessages = append(globalMessages, context)
			return nil
		})

	// Workflow-specific handler for "target-workflow"
	runner.Broker.RegisterWorkflowHandler(MessageTypeStorePut, "target-workflow",
		func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
			workflowMessages = append(workflowMessages, context)
			return nil
		})

	// Workflow-specific handler for "other-workflow"
	runner.Broker.RegisterWorkflowHandler(MessageTypeStorePut, "other-workflow",
		func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
			otherWorkflowMessages = append(otherWorkflowMessages, context)
			return nil
		})

	// Create and execute target workflow
	targetWorkflow := NewWorkflow("target-workflow", "Target Workflow", "1.0.0")
	stage := NewStage("stage-1", "Stage", "Stage")
	testAction := &TestContextMessageAction{BaseAction: NewBaseAction("action", "Action")}
	stage.AddAction(testAction)
	targetWorkflow.AddStage(stage)

	err := runner.Execute(context.Background(), targetWorkflow, &TestLogger{t: t})
	assert.NoError(t, err)

	// Create and execute different workflow
	otherWorkflow := NewWorkflow("different-workflow", "Different Workflow", "1.0.0")
	stage2 := NewStage("stage-2", "Stage 2", "Stage 2")
	testAction2 := &TestContextMessageAction{BaseAction: NewBaseAction("action2", "Action 2")}
	stage2.AddAction(testAction2)
	otherWorkflow.AddStage(stage2)

	err = runner.Execute(context.Background(), otherWorkflow, &TestLogger{t: t})
	assert.NoError(t, err)

	// Verify message routing
	assert.Len(t, globalMessages, 2)        // Global handler gets all messages
	assert.Len(t, workflowMessages, 1)      // Workflow-specific handler gets only target workflow
	assert.Len(t, otherWorkflowMessages, 0) // Other workflow handler gets none

	// Verify the workflow-specific message is correct
	assert.Equal(t, "target-workflow", workflowMessages[0].WorkflowID)
	assert.Equal(t, "stage-1", workflowMessages[0].StageID)
	assert.Equal(t, "action", workflowMessages[0].ActionName)
}

// TestContextMessaging_StageSpecificHandlerNormalWorkflow tests stage-specific
// handlers work with normal workflows
func TestContextMessaging_StageSpecificHandlerNormalWorkflow(t *testing.T) {
	runner := NewRunner()

	var targetStageMessages []MessageContext
	var otherStageMessages []MessageContext

	// Stage-specific handler for specific stage
	runner.Broker.RegisterStageHandler(MessageTypeStorePut, "test-workflow", "target-stage",
		func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
			targetStageMessages = append(targetStageMessages, context)
			return nil
		})

	// Stage-specific handler for different stage
	runner.Broker.RegisterStageHandler(MessageTypeStorePut, "test-workflow", "other-stage",
		func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
			otherStageMessages = append(otherStageMessages, context)
			return nil
		})

	// Create workflow with multiple stages
	workflow := NewWorkflow("test-workflow", "Test Workflow", "1.0.0")

	targetStage := NewStage("target-stage", "Target Stage", "Target")
	targetStage.AddAction(&TestContextMessageAction{BaseAction: NewBaseAction("action1", "Action 1")})
	workflow.AddStage(targetStage)

	differentStage := NewStage("different-stage", "Different Stage", "Different")
	differentStage.AddAction(&TestContextMessageAction{BaseAction: NewBaseAction("action2", "Action 2")})
	workflow.AddStage(differentStage)

	err := runner.Execute(context.Background(), workflow, &TestLogger{t: t})
	assert.NoError(t, err)

	// Verify stage-specific routing
	assert.Len(t, targetStageMessages, 1) // Only target stage messages
	assert.Len(t, otherStageMessages, 0)  // No messages from other stage

	// Verify the message context
	assert.Equal(t, "test-workflow", targetStageMessages[0].WorkflowID)
	assert.Equal(t, "target-stage", targetStageMessages[0].StageID)
	assert.Equal(t, "action1", targetStageMessages[0].ActionName)
}

// TestContextMessaging_ActionSpecificHandlerNormalWorkflow tests action-specific
// handlers work with normal workflows
func TestContextMessaging_ActionSpecificHandlerNormalWorkflow(t *testing.T) {
	runner := NewRunner()

	var targetActionMessages []MessageContext
	var otherActionMessages []MessageContext

	// Action-specific handlers
	runner.Broker.RegisterActionHandler(MessageTypeStorePut, "test-workflow", "test-stage", "target-action",
		func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
			targetActionMessages = append(targetActionMessages, context)
			return nil
		})

	runner.Broker.RegisterActionHandler(MessageTypeStorePut, "test-workflow", "test-stage", "other-action",
		func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
			otherActionMessages = append(otherActionMessages, context)
			return nil
		})

	// Create workflow with multiple actions in same stage
	workflow := NewWorkflow("test-workflow", "Test Workflow", "1.0.0")
	stage := NewStage("test-stage", "Test Stage", "Test")

	stage.AddAction(&TestContextMessageAction{BaseAction: NewBaseAction("target-action", "Target Action")})
	stage.AddAction(&TestContextMessageAction{BaseAction: NewBaseAction("different-action", "Different Action")})

	workflow.AddStage(stage)

	err := runner.Execute(context.Background(), workflow, &TestLogger{t: t})
	assert.NoError(t, err)

	// Verify action-specific routing
	assert.Len(t, targetActionMessages, 1) // Only target action messages
	assert.Len(t, otherActionMessages, 0)  // No messages from other action

	// Verify context
	assert.Equal(t, "test-workflow", targetActionMessages[0].WorkflowID)
	assert.Equal(t, "test-stage", targetActionMessages[0].StageID)
	assert.Equal(t, "target-action", targetActionMessages[0].ActionName)
	assert.Equal(t, int32(0), targetActionMessages[0].ActionIndex) // First action
	assert.False(t, targetActionMessages[0].IsLastAction)          // Not last action
}

// TestContextMessaging_LegacyAndContextHandlersCoexist tests that legacy handlers
// and context handlers can coexist in normal workflows
func TestContextMessaging_LegacyAndContextHandlersCoexist(t *testing.T) {
	runner := NewRunner()

	var legacyHandlerCalled bool
	var contextHandlerCalled bool
	var contextReceived MessageContext

	// Legacy handler (no context)
	runner.Broker.RegisterHandler(MessageTypeStorePut,
		func(msgType MessageType, payload json.RawMessage) error {
			legacyHandlerCalled = true
			return nil
		})

	// Context handler
	runner.Broker.RegisterHandlerWithContext(MessageTypeStorePut,
		func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
			contextHandlerCalled = true
			contextReceived = context
			return nil
		})

	// Execute normal workflow
	workflow := NewWorkflow("coexist-workflow", "Coexist Test", "1.0.0")
	stage := NewStage("stage", "Stage", "Stage")
	stage.AddAction(&TestContextMessageAction{BaseAction: NewBaseAction("action", "Action")})
	workflow.AddStage(stage)

	err := runner.Execute(context.Background(), workflow, &TestLogger{t: t})
	assert.NoError(t, err)

	// Verify both handlers were called
	assert.True(t, legacyHandlerCalled, "Legacy handler should be called")
	assert.True(t, contextHandlerCalled, "Context handler should be called")

	// Verify context was received properly
	assert.Equal(t, "coexist-workflow", contextReceived.WorkflowID)
	assert.Equal(t, "stage", contextReceived.StageID)
	assert.Equal(t, "action", contextReceived.ActionName)
	assert.False(t, contextReceived.IsChildProcess)
}
