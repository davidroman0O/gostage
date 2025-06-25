package gostage

import (
	"context"
	"encoding/json"
	"fmt"
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

// RunnerBroker handles message sending, receiving, and routing between processes
// It uses gRPC transport directly for all inter-process communication
type RunnerBroker struct {
	transport *GRPCTransport
}

// NewRunnerBroker creates a new broker for IPC communication using gRPC transport
func NewRunnerBroker() *RunnerBroker {
	// Create default gRPC transport
	grpcTransport, err := NewGRPCTransport("localhost", 0)
	if err != nil {
		panic(fmt.Sprintf("Failed to create default gRPC transport: %v", err))
	}

	return &RunnerBroker{
		transport: grpcTransport,
	}
}

// NewRunnerBrokerWithTransport creates a new broker with a custom gRPC transport
func NewRunnerBrokerWithTransport(transport *GRPCTransport) *RunnerBroker {
	return &RunnerBroker{
		transport: transport,
	}
}

// NewRunnerBrokerFromConfig creates a broker based on gRPC configuration
func NewRunnerBrokerFromConfig() (*RunnerBroker, error) {
	address, port := GetGRPCAddressFromEnv()
	grpcTransport, err := NewGRPCTransport(address, port)
	if err != nil {
		return nil, err
	}

	return NewRunnerBrokerWithTransport(grpcTransport), nil
}

// RegisterHandler registers a handler for a specific message type.
func (b *RunnerBroker) RegisterHandler(msgType MessageType, handler MessageHandler) {
	b.transport.SetHandler(msgType, handler)
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
