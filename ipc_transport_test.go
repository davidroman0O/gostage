package gostage

import (
	"testing"
)

func TestGRPCTransportCreation(t *testing.T) {
	// Test gRPC transport creation
	transport, err := NewGRPCTransport("localhost", 0)
	if err != nil {
		t.Fatalf("Failed to create gRPC transport: %v", err)
	}

	// Clean up
	transport.Close()
}

func TestGRPCTransportConfiguration(t *testing.T) {
	address := "localhost"
	port := 0 // Auto-assign port

	transport, err := NewGRPCTransport(address, port)
	if err != nil {
		t.Fatalf("Failed to create gRPC transport: %v", err)
	}
	defer transport.Close()

	if transport.address != address {
		t.Errorf("Expected address %s, got %s", address, transport.address)
	}

	// Start the server to get the actual assigned port
	if err := transport.StartServer(); err != nil {
		t.Fatalf("Failed to start gRPC server: %v", err)
	}

	// The actual port should be assigned after starting the server
	actualPort := transport.GetActualPort()
	if actualPort == 0 {
		t.Error("Expected actual port to be assigned after starting server, got 0")
	}
}

func TestRunnerBrokerWithGRPCTransport(t *testing.T) {
	// Test RunnerBroker with gRPC transport
	transport, err := NewGRPCTransport("localhost", 0)
	if err != nil {
		t.Fatalf("Failed to create gRPC transport: %v", err)
	}

	broker := NewRunnerBrokerWithTransport(transport)
	defer broker.Close()

	if broker.GetTransport() != transport {
		t.Error("Expected broker to use the provided gRPC transport")
	}
}

func TestRunnerBrokerFromConfig(t *testing.T) {
	// Test creating broker from config
	broker, err := NewRunnerBrokerFromConfig()
	if err != nil {
		t.Fatalf("Failed to create broker from config: %v", err)
	}
	defer broker.Close()

	// Should create a gRPC transport by default
	if broker.GetTransport() == nil {
		t.Error("Expected broker to have a gRPC transport")
	}
}

func TestGRPCAddressFromEnv(t *testing.T) {
	// Test getting gRPC address from environment
	address, port := GetGRPCAddressFromEnv()
	if address != "localhost" {
		t.Errorf("Expected default address 'localhost', got %s", address)
	}
	if port != 50051 {
		t.Errorf("Expected default port 50051, got %d", port)
	}
}
