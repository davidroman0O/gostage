package gostage

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

// JSONTransport implements IPCTransport using JSON over stdin/stdout
// This is a refactor of the existing RunnerBroker code to fit the transport interface
type JSONTransport struct {
	mu               sync.RWMutex
	output           io.Writer
	handlers         map[MessageType]MessageHandler
	defaultHandler   MessageHandler
	middleware       []IPCMiddleware
	messageCallbacks []func(MessageType, json.RawMessage) error
}

// NewJSONTransport creates a new JSON-based transport
func NewJSONTransport(output io.Writer) *JSONTransport {
	return &JSONTransport{
		output:           output,
		handlers:         make(map[MessageType]MessageHandler),
		middleware:       make([]IPCMiddleware, 0),
		messageCallbacks: make([]func(MessageType, json.RawMessage) error, 0),
	}
}

// Send sends a message through the JSON transport with middleware processing
func (j *JSONTransport) Send(msgType MessageType, payload interface{}) error {
	j.mu.RLock()
	middleware := make([]IPCMiddleware, len(j.middleware))
	copy(middleware, j.middleware)
	j.mu.RUnlock()

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

	_, err = j.output.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	return nil
}

// Listen reads and processes messages from the given reader through middleware
func (j *JSONTransport) Listen(reader io.Reader) error {
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
		j.mu.RLock()
		middleware := make([]IPCMiddleware, len(j.middleware))
		copy(middleware, j.middleware)
		j.mu.RUnlock()

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
		j.mu.RLock()
		handler, exists := j.handlers[currentType]
		if !exists {
			handler = j.defaultHandler
		}
		callbacks := make([]func(MessageType, json.RawMessage) error, len(j.messageCallbacks))
		copy(callbacks, j.messageCallbacks)
		j.mu.RUnlock()

		if handler != nil {
			if err := handler(currentType, currentPayload); err != nil {
				// Log the error but continue processing other messages
				fmt.Fprintf(io.Discard, "Handler error for message type %s: %v\n", currentType, err)
			}
		}

		// Call message callbacks
		for _, callback := range callbacks {
			if err := callback(currentType, currentPayload); err != nil {
				fmt.Fprintf(io.Discard, "Message callback error for message type %s: %v\n", currentType, err)
			}
		}
	}
}

// SetHandler registers a handler for a specific message type
func (j *JSONTransport) SetHandler(msgType MessageType, handler MessageHandler) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.handlers[msgType] = handler
}

// SetDefaultHandler sets a handler for any message types that are not explicitly registered
func (j *JSONTransport) SetDefaultHandler(handler MessageHandler) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.defaultHandler = handler
}

// AddMiddleware adds IPC middleware to the transport
func (j *JSONTransport) AddMiddleware(middleware ...IPCMiddleware) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.middleware = append(j.middleware, middleware...)
}

// AddMessageCallback adds a callback that will be called for every received message
func (j *JSONTransport) AddMessageCallback(callback func(MessageType, json.RawMessage) error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.messageCallbacks = append(j.messageCallbacks, callback)
}

// Close closes the JSON transport (no-op for this implementation)
func (j *JSONTransport) Close() error {
	return nil
}

// GetType returns the transport type
func (j *JSONTransport) GetType() TransportType {
	return TransportJSON
}
