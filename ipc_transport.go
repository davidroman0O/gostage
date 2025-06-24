package gostage

import (
	"encoding/json"
	"io"
	"os"
	"strconv"
)

// TransportType defines the available IPC transport types
type TransportType string

const (
	TransportJSON TransportType = "json"
	TransportGRPC TransportType = "grpc"
)

// IPCTransport defines the interface for different IPC transport mechanisms
// This abstraction allows switching between JSON/stdin-stdout and gRPC
// while keeping the existing API unchanged
type IPCTransport interface {
	// Send sends a message with the specified type and payload
	Send(msgType MessageType, payload interface{}) error

	// Listen starts listening for incoming messages and processes them through handlers
	Listen(reader io.Reader) error

	// SetHandler registers a handler for a specific message type
	SetHandler(msgType MessageType, handler MessageHandler)

	// SetDefaultHandler sets the default handler for unregistered message types
	SetDefaultHandler(handler MessageHandler)

	// AddMiddleware adds IPC middleware to the transport
	AddMiddleware(middleware ...IPCMiddleware)

	// AddMessageCallback adds a callback that will be called for every received message
	AddMessageCallback(callback func(MessageType, json.RawMessage) error)

	// Close closes the transport and cleans up resources
	Close() error

	// GetType returns the transport type
	GetType() TransportType
}

// TransportConfig holds configuration for creating transports
type TransportConfig struct {
	Type        TransportType
	Output      io.Writer // For JSON transport
	GRPCAddress string    // For gRPC transport (e.g., "localhost:50051")
	GRPCPort    int       // For gRPC transport
}

// NewIPCTransport creates a new transport based on the configuration
func NewIPCTransport(config TransportConfig) (IPCTransport, error) {
	switch config.Type {
	case TransportJSON:
		output := config.Output
		if output == nil {
			output = os.Stdout // Default to stdout for child processes
		}
		return NewJSONTransport(output), nil
	case TransportGRPC:
		return NewGRPCTransport(config.GRPCAddress, config.GRPCPort)
	default:
		output := config.Output
		if output == nil {
			output = os.Stdout // Default to stdout for child processes
		}
		return NewJSONTransport(output), nil // Default to JSON
	}
}

// GetTransportTypeFromEnv returns the transport type from environment variable
// Defaults to JSON if not set or invalid
func GetTransportTypeFromEnv() TransportType {
	switch os.Getenv("GOSTAGE_IPC_TRANSPORT") {
	case "grpc":
		return TransportGRPC
	case "json":
		return TransportJSON
	default:
		return TransportJSON
	}
}

// GetGRPCAddressFromEnv returns the gRPC address from environment
// Defaults to localhost:50051
func GetGRPCAddressFromEnv() (string, int) {
	address := os.Getenv("GOSTAGE_GRPC_ADDRESS")
	if address == "" {
		address = "localhost"
	}

	portStr := os.Getenv("GOSTAGE_GRPC_PORT")
	port := 50051 // Default port
	if portStr != "" {
		if p, err := strconv.Atoi(portStr); err == nil {
			port = p
		}
	}

	return address, port
}
