package gostage

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// RunnerBroker handles message sending, receiving, and routing between processes
// It uses gRPC transport directly for all inter-process communication
type RunnerBroker struct {
	transport           *GRPCTransport
	contextHandlers     map[string]ContextMessageHandler // Key: "msgType:workflowID:stageID:actionName"
	sessionID           string
	sequenceNumber      int64
	sequenceNumberMutex sync.Mutex
}

// generateSessionID creates a unique session ID with timestamp and random component
func generateSessionID() string {
	// Get current timestamp
	timestamp := time.Now().UnixNano()

	// Add random bytes for uniqueness
	randomBytes := make([]byte, 4)
	if _, err := rand.Read(randomBytes); err != nil {
		// Fallback to PID if random generation fails
		return fmt.Sprintf("session-%d-%d", timestamp, os.Getpid())
	}

	randomHex := hex.EncodeToString(randomBytes)
	return fmt.Sprintf("session-%d-%s", timestamp, randomHex)
}

// NewRunnerBroker creates a new broker for IPC communication using gRPC transport
func NewRunnerBroker() *RunnerBroker {
	// Create default gRPC transport
	grpcTransport, err := NewGRPCTransport("localhost", 0)
	if err != nil {
		panic(fmt.Sprintf("Failed to create default gRPC transport: %v", err))
	}

	return &RunnerBroker{
		transport:       grpcTransport,
		contextHandlers: make(map[string]ContextMessageHandler),
		sessionID:       generateSessionID(),
	}
}

// NewRunnerBrokerWithTransport creates a new broker with a custom gRPC transport
func NewRunnerBrokerWithTransport(transport *GRPCTransport) *RunnerBroker {
	return &RunnerBroker{
		transport:       transport,
		contextHandlers: make(map[string]ContextMessageHandler),
		sessionID:       generateSessionID(),
	}
}

// NewRunnerBrokerFromConfig creates a broker based on gRPC configuration
func NewRunnerBrokerFromConfig() (*RunnerBroker, error) {
	address, port := GetGRPCAddressFromEnv()
	grpcTransport, err := NewGRPCTransport(address, port)
	if err != nil {
		return nil, err
	}

	return &RunnerBroker{
		transport:       grpcTransport,
		contextHandlers: make(map[string]ContextMessageHandler),
		sessionID:       generateSessionID(),
	}, nil
}

// RegisterHandler registers a handler for a specific message type (legacy - no context metadata).
func (b *RunnerBroker) RegisterHandler(msgType MessageType, handler MessageHandler) {
	b.transport.SetHandler(msgType, handler)
}

// RegisterHandlerWithContext registers a global handler that receives context metadata
func (b *RunnerBroker) RegisterHandlerWithContext(msgType MessageType, handler ContextMessageHandler) {
	key := fmt.Sprintf("%s:::", msgType)
	b.contextHandlers[key] = handler
}

// RegisterWorkflowHandler registers a handler for a specific workflow
func (b *RunnerBroker) RegisterWorkflowHandler(msgType MessageType, workflowID string, handler ContextMessageHandler) {
	key := fmt.Sprintf("%s:%s::", msgType, workflowID)
	b.contextHandlers[key] = handler
}

// RegisterStageHandler registers a handler for a specific stage within a workflow
func (b *RunnerBroker) RegisterStageHandler(msgType MessageType, workflowID, stageID string, handler ContextMessageHandler) {
	key := fmt.Sprintf("%s:%s:%s:", msgType, workflowID, stageID)
	b.contextHandlers[key] = handler
}

// RegisterActionHandler registers a handler for a specific action
func (b *RunnerBroker) RegisterActionHandler(msgType MessageType, workflowID, stageID, actionName string, handler ContextMessageHandler) {
	key := fmt.Sprintf("%s:%s:%s:%s", msgType, workflowID, stageID, actionName)
	b.contextHandlers[key] = handler
}

// SetDefaultHandler sets a handler for any message types that are not explicitly registered.
func (b *RunnerBroker) SetDefaultHandler(handler MessageHandler) {
	b.transport.SetDefaultHandler(handler)
}

// AddIPCMiddleware adds IPC middleware to the broker
func (b *RunnerBroker) AddIPCMiddleware(middleware ...IPCMiddleware) {
	b.transport.AddMiddleware(middleware...)
}

// AddMessageCallback adds a callback that will be called for every received message
func (b *RunnerBroker) AddMessageCallback(callback func(MessageType, json.RawMessage) error) {
	b.transport.AddMessageCallback(callback)
}

// Send sends a message through the underlying gRPC transport
func (b *RunnerBroker) Send(msgType MessageType, payload interface{}) error {
	return b.transport.Send(msgType, payload)
}

// SendWithContext sends a message with context metadata from ActionContext
func (b *RunnerBroker) SendWithContext(msgType MessageType, payload interface{}, actionCtx *ActionContext) error {
	// Generate sequence number
	b.sequenceNumberMutex.Lock()
	b.sequenceNumber++
	seqNum := b.sequenceNumber
	b.sequenceNumberMutex.Unlock()

	// Build context metadata from ActionContext
	context := MessageContext{
		WorkflowID:     actionCtx.Workflow.ID,
		StageID:        actionCtx.Stage.ID,
		ActionName:     actionCtx.Action.Name(),
		ProcessID:      int32(os.Getpid()),
		IsChildProcess: b.isChildProcess(),
		ActionIndex:    int32(actionCtx.ActionIndex),
		IsLastAction:   actionCtx.IsLastAction,
		SessionID:      b.sessionID,
		SequenceNumber: seqNum,
	}

	// For normal (non-spawned) workflows, handle messages locally
	if !b.isChildProcess() && !b.transport.IsClientReady() {
		return b.handleLocalMessage(msgType, payload, context)
	}

	// For spawned workflows, use gRPC transport
	return b.transport.SendWithContext(msgType, payload, context)
}

// handleLocalMessage processes messages locally for normal workflows
func (b *RunnerBroker) handleLocalMessage(msgType MessageType, payload interface{}, context MessageContext) error {
	// Convert payload to JSON for consistency with gRPC path
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	var handlerErr error

	// Call ALL applicable context handlers (from most specific to least specific)
	handlers := b.findAllContextHandlers(msgType, context)
	for _, handler := range handlers {
		if err := handler(msgType, payloadBytes, context); err != nil && handlerErr == nil {
			handlerErr = err
		}
	}

	// Also call legacy transport handlers for backward compatibility
	if b.transport != nil {
		// Use the transport's direct handler mechanism
		b.transport.mu.RLock()
		handler, exists := b.transport.handlers[msgType]
		if !exists {
			handler = b.transport.defaultHandler
		}
		b.transport.mu.RUnlock()

		if handler != nil {
			if legacyErr := handler(msgType, payloadBytes); legacyErr != nil && handlerErr == nil {
				handlerErr = legacyErr
			}
		}
	}

	return handlerErr
}

// findAllContextHandlers returns ALL applicable context handlers for the message
// in order from most specific to least specific
func (b *RunnerBroker) findAllContextHandlers(msgType MessageType, context MessageContext) []ContextMessageHandler {
	var handlers []ContextMessageHandler

	// Check action-specific handler first (most specific)
	key := fmt.Sprintf("%s:%s:%s:%s", msgType, context.WorkflowID, context.StageID, context.ActionName)
	if handler, exists := b.contextHandlers[key]; exists {
		handlers = append(handlers, handler)
	}

	// Check stage-specific handler
	key = fmt.Sprintf("%s:%s:%s:", msgType, context.WorkflowID, context.StageID)
	if handler, exists := b.contextHandlers[key]; exists {
		handlers = append(handlers, handler)
	}

	// Check workflow-specific handler
	key = fmt.Sprintf("%s:%s::", msgType, context.WorkflowID)
	if handler, exists := b.contextHandlers[key]; exists {
		handlers = append(handlers, handler)
	}

	// Check global context handler (least specific)
	key = fmt.Sprintf("%s:::", msgType)
	if handler, exists := b.contextHandlers[key]; exists {
		handlers = append(handlers, handler)
	}

	return handlers
}

// SendWithCustomContext sends a message with custom context metadata
func (b *RunnerBroker) SendWithCustomContext(msgType MessageType, payload interface{}, actionCtx *ActionContext, customContext map[string]interface{}) error {
	// Generate sequence number
	b.sequenceNumberMutex.Lock()
	b.sequenceNumber++
	seqNum := b.sequenceNumber
	b.sequenceNumberMutex.Unlock()

	// Build context with custom overrides
	context := MessageContext{
		WorkflowID:     actionCtx.Workflow.ID,
		StageID:        actionCtx.Stage.ID,
		ActionName:     actionCtx.Action.Name(),
		ProcessID:      int32(os.Getpid()),
		IsChildProcess: b.isChildProcess(),
		ActionIndex:    int32(actionCtx.ActionIndex),
		IsLastAction:   actionCtx.IsLastAction,
		SessionID:      b.sessionID,
		SequenceNumber: seqNum,
	}

	// Apply custom overrides
	if val, ok := customContext["workflow_id"].(string); ok {
		context.WorkflowID = val
	}
	if val, ok := customContext["stage_id"].(string); ok {
		context.StageID = val
	}
	if val, ok := customContext["action_name"].(string); ok {
		context.ActionName = val
	}

	// For normal (non-spawned) workflows, handle messages locally
	if !b.isChildProcess() && !b.transport.IsClientReady() {
		return b.handleLocalMessage(msgType, payload, context)
	}

	// For spawned workflows, use gRPC transport
	return b.transport.SendWithContext(msgType, payload, context)
}

// isChildProcess determines if this broker is running in a child process
func (b *RunnerBroker) isChildProcess() bool {
	// Check for child process indicators
	for _, arg := range os.Args[1:] {
		if arg == "--gostage-child" {
			return true
		}
	}
	return false
}

// Close closes the underlying gRPC transport
func (b *RunnerBroker) Close() error {
	return b.transport.Close()
}

// GetTransport returns the underlying gRPC transport
func (b *RunnerBroker) GetTransport() *GRPCTransport {
	return b.transport
}
