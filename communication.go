package gostage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
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
)

// Message is the standard unit of communication between a parent and child process.
type Message struct {
	Type    MessageType     `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// MessageHandler is a function that processes a received message.
type MessageHandler func(msgType MessageType, payload json.RawMessage) error

// RunnerBroker handles message sending, receiving, and routing between processes
type RunnerBroker struct {
	mu               sync.RWMutex
	output           io.Writer
	handlers         map[MessageType]MessageHandler
	defaultHandler   MessageHandler
	middleware       []IPCMiddleware                            // IPC middleware chain
	messageCallbacks []func(MessageType, json.RawMessage) error // Callbacks for middleware hooks
}

// NewRunnerBroker creates a new broker for IPC communication
func NewRunnerBroker(output io.Writer) *RunnerBroker {
	return &RunnerBroker{
		output:           output,
		handlers:         make(map[MessageType]MessageHandler),
		middleware:       make([]IPCMiddleware, 0),
		messageCallbacks: make([]func(MessageType, json.RawMessage) error, 0),
	}
}

// RegisterHandler registers a handler for a specific message type.
func (b *RunnerBroker) RegisterHandler(msgType MessageType, handler MessageHandler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[msgType] = handler
}

// SetDefaultHandler sets a handler for any message types that are not explicitly registered.
func (b *RunnerBroker) SetDefaultHandler(handler MessageHandler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.defaultHandler = handler
}

// AddIPCMiddleware adds IPC middleware to the broker
func (b *RunnerBroker) AddIPCMiddleware(middleware ...IPCMiddleware) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.middleware = append(b.middleware, middleware...)
}

// AddMessageCallback adds a callback that will be called for every received message
func (b *RunnerBroker) AddMessageCallback(callback func(MessageType, json.RawMessage) error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.messageCallbacks = append(b.messageCallbacks, callback)
}

// Send sends a message through the IPC middleware chain
func (b *RunnerBroker) Send(msgType MessageType, payload interface{}) error {
	b.mu.RLock()
	middleware := make([]IPCMiddleware, len(b.middleware))
	copy(middleware, b.middleware)
	b.mu.RUnlock()

	// Process through outbound middleware chain
	currentType := msgType
	currentPayload := payload
	var err error

	for _, mw := range middleware {
		currentType, currentPayload, err = mw.ProcessOutbound(currentType, currentPayload)
		if err != nil {
			return fmt.Errorf("IPC middleware error: %w", err)
		}
	}

	// Create the message
	payloadBytes, err := json.Marshal(currentPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	msg := Message{
		Type:    currentType,
		Payload: json.RawMessage(payloadBytes),
	}

	// Marshal and send
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	data = append(data, '\n')

	_, err = b.output.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	return nil
}

// Listen reads and processes messages from the given reader through middleware
func (b *RunnerBroker) Listen(reader io.Reader) error {
	decoder := json.NewDecoder(reader)

	for {
		var msg Message
		if err := decoder.Decode(&msg); err != nil {
			if err == io.EOF {
				return nil // Normal end of stream
			}
			return fmt.Errorf("failed to decode message: %w", err)
		}

		// Process through inbound middleware chain
		b.mu.RLock()
		middleware := make([]IPCMiddleware, len(b.middleware))
		copy(middleware, b.middleware)
		b.mu.RUnlock()

		currentType := msg.Type
		currentPayload := msg.Payload
		var err error

		for _, mw := range middleware {
			currentType, currentPayload, err = mw.ProcessInbound(currentType, currentPayload)
			if err != nil {
				return fmt.Errorf("IPC middleware inbound error: %w", err)
			}
		}

		// Find and execute handler
		b.mu.RLock()
		handler, exists := b.handlers[currentType]
		if !exists {
			handler = b.defaultHandler
		}
		b.mu.RUnlock()

		if handler != nil {
			if err := handler(currentType, currentPayload); err != nil {
				// Log the error but continue processing other messages
				fmt.Fprintf(os.Stderr, "Handler error for message type %s: %v\n", currentType, err)
			}
		}

		// Call message callbacks
		for _, callback := range b.messageCallbacks {
			if err := callback(currentType, currentPayload); err != nil {
				fmt.Fprintf(os.Stderr, "Message callback error for message type %s: %v\n", currentType, err)
			}
		}
	}
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
