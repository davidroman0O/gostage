package gostage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// MessageType is a string that defines the purpose of a message.
type MessageType string

const (
	// MessageTypeLog is for sending log messages between processes.
	MessageTypeLog MessageType = "log"
	// MessageTypeStorePut is for synchronizing a single store.Put operation.
	MessageTypeStorePut MessageType = "store_put"
	// MessageTypeStoreDelete is for synchronizing a single store.Delete operation.
	MessageTypeStoreDelete MessageType = "store_delete"
	// MessageTypeWorkflowStart is the initial message from parent to child to start execution.
	MessageTypeWorkflowStart MessageType = "workflow_start"
	// MessageTypeWorkflowResult is the final message from child to parent with the outcome.
	MessageTypeWorkflowResult MessageType = "workflow_result"
	// MessageTypeFinalStore is sent from child to parent with the complete final store state.
	MessageTypeFinalStore MessageType = "final_store"
)

// Message is the standard unit of communication between a parent and child process.
// This is kept for backwards compatibility with existing JSON-based communication.
type Message struct {
	Type    MessageType     `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// MessageHandler is a function that processes a received message.
type MessageHandler func(msgType MessageType, payload json.RawMessage) error

// ContextMessageHandler processes messages with full context metadata
type ContextMessageHandler func(msgType MessageType, payload json.RawMessage, context MessageContext) error

// MessageContext provides comprehensive information about message source
type MessageContext struct {
	WorkflowID     string
	StageID        string
	ActionName     string
	ProcessID      int32
	IsChildProcess bool
	ActionIndex    int32
	IsLastAction   bool
	SessionID      string
	SequenceNumber int64
}

// HandlerRegistration defines how a handler should be registered
type HandlerRegistration struct {
	MessageType MessageType
	Handler     MessageHandler
	WorkflowID  string // Empty string means global handler
	StageID     string // Empty string means any stage
	ActionName  string // Empty string means any action
}

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

// IPCMiddleware allows customization of inter-process communication
type IPCMiddleware interface {
	// ProcessOutbound is called before sending a message from child to parent
	ProcessOutbound(msgType MessageType, payload interface{}) (MessageType, interface{}, error)

	// ProcessInbound is called when parent receives a message from child
	ProcessInbound(msgType MessageType, payload json.RawMessage) (MessageType, json.RawMessage, error)
}

// IPCHandler handles IPC messages with middleware support
type IPCHandler func(msgType MessageType, payload json.RawMessage) error

// SpawnMiddleware provides hooks for spawn process lifecycle and communication
type SpawnMiddleware interface {
	// BeforeSpawn is called before creating a child process
	BeforeSpawn(ctx context.Context, def SubWorkflowDef) (context.Context, SubWorkflowDef, error)

	// AfterSpawn is called after child process completes (success or failure)
	AfterSpawn(ctx context.Context, def SubWorkflowDef, err error) error

	// OnChildMessage is called when parent receives any message from child
	OnChildMessage(msgType MessageType, payload json.RawMessage) error
}

// IPCMiddlewareFunc is a function adapter for IPCMiddleware
type IPCMiddlewareFunc struct {
	ProcessOutboundFunc func(MessageType, interface{}) (MessageType, interface{}, error)
	ProcessInboundFunc  func(MessageType, json.RawMessage) (MessageType, json.RawMessage, error)
}

func (f IPCMiddlewareFunc) ProcessOutbound(msgType MessageType, payload interface{}) (MessageType, interface{}, error) {
	if f.ProcessOutboundFunc != nil {
		return f.ProcessOutboundFunc(msgType, payload)
	}
	return msgType, payload, nil
}

func (f IPCMiddlewareFunc) ProcessInbound(msgType MessageType, payload json.RawMessage) (MessageType, json.RawMessage, error) {
	if f.ProcessInboundFunc != nil {
		return f.ProcessInboundFunc(msgType, payload)
	}
	return msgType, payload, nil
}

// SpawnMiddlewareFunc is a function adapter for SpawnMiddleware
type SpawnMiddlewareFunc struct {
	BeforeSpawnFunc    func(context.Context, SubWorkflowDef) (context.Context, SubWorkflowDef, error)
	AfterSpawnFunc     func(context.Context, SubWorkflowDef, error) error
	OnChildMessageFunc func(MessageType, json.RawMessage) error
}

func (f SpawnMiddlewareFunc) BeforeSpawn(ctx context.Context, def SubWorkflowDef) (context.Context, SubWorkflowDef, error) {
	if f.BeforeSpawnFunc != nil {
		return f.BeforeSpawnFunc(ctx, def)
	}
	return ctx, def, nil
}

func (f SpawnMiddlewareFunc) AfterSpawn(ctx context.Context, def SubWorkflowDef, err error) error {
	if f.AfterSpawnFunc != nil {
		return f.AfterSpawnFunc(ctx, def, err)
	}
	return nil
}

func (f SpawnMiddlewareFunc) OnChildMessage(msgType MessageType, payload json.RawMessage) error {
	if f.OnChildMessageFunc != nil {
		return f.OnChildMessageFunc(msgType, payload)
	}
	return nil
}
