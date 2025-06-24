package gostage

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

func TestJSONTransportBasics(t *testing.T) {
	var output bytes.Buffer
	transport := NewJSONTransport(&output)

	// Test transport type
	if transport.GetType() != TransportJSON {
		t.Errorf("Expected JSON transport type, got %s", transport.GetType())
	}

	// Test sending a log message
	err := transport.Send(MessageTypeLog, map[string]interface{}{
		"level":     "info",
		"message":   "Test log message",
		"timestamp": time.Now().Unix(),
	})

	if err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}

	// Verify the output contains expected JSON
	outputStr := output.String()
	if !strings.Contains(outputStr, `"type":"log"`) {
		t.Errorf("Output should contain message type, got: %s", outputStr)
	}
	if !strings.Contains(outputStr, `"Test log message"`) {
		t.Errorf("Output should contain message content, got: %s", outputStr)
	}

	t.Logf("✅ JSON Transport Output: %s", strings.TrimSpace(outputStr))
}

func TestGRPCTransportCreation(t *testing.T) {
	transport, err := NewGRPCTransport("localhost", 50051)
	if err != nil {
		t.Fatalf("Failed to create gRPC transport: %v", err)
	}
	defer transport.Close()

	// Test transport type
	if transport.GetType() != TransportGRPC {
		t.Errorf("Expected gRPC transport type, got %s", transport.GetType())
	}

	t.Logf("✅ gRPC Transport created successfully")
}

func TestMessageTypeConversion(t *testing.T) {
	testCases := []struct {
		stringType MessageType
		want       string
	}{
		{MessageTypeLog, "MESSAGE_TYPE_LOG"},
		{MessageTypeStorePut, "MESSAGE_TYPE_STORE_PUT"},
		{MessageTypeStoreDelete, "MESSAGE_TYPE_STORE_DELETE"},
		{MessageTypeWorkflowStart, "MESSAGE_TYPE_WORKFLOW_START"},
		{MessageTypeWorkflowResult, "MESSAGE_TYPE_WORKFLOW_RESULT"},
		{MessageTypeFinalStore, "MESSAGE_TYPE_FINAL_STORE"},
	}

	for _, tc := range testCases {
		// Test string -> proto conversion
		protoType := convertToProtoMessageType(tc.stringType)
		if protoType.String() != tc.want {
			t.Errorf("String->Proto: expected %s, got %s", tc.want, protoType.String())
		}

		// Test round-trip conversion
		backToString := convertFromProtoMessageType(protoType)
		if backToString != tc.stringType {
			t.Errorf("Round-trip failed: %s -> %s -> %s", tc.stringType, protoType.String(), backToString)
		}
	}

	t.Logf("✅ All message type conversions working correctly")
}

func TestTransportFactory(t *testing.T) {
	// Test JSON transport creation
	jsonConfig := TransportConfig{
		Type:   TransportJSON,
		Output: os.Stdout,
	}

	jsonTransport, err := NewIPCTransport(jsonConfig)
	if err != nil {
		t.Fatalf("Failed to create JSON transport via factory: %v", err)
	}
	defer jsonTransport.Close()

	if jsonTransport.GetType() != TransportJSON {
		t.Errorf("Factory should create JSON transport")
	}

	// Test gRPC transport creation
	grpcConfig := TransportConfig{
		Type:        TransportGRPC,
		GRPCAddress: "localhost",
		GRPCPort:    50053,
	}

	grpcTransport, err := NewIPCTransport(grpcConfig)
	if err != nil {
		t.Fatalf("Failed to create gRPC transport via factory: %v", err)
	}
	defer grpcTransport.Close()

	if grpcTransport.GetType() != TransportGRPC {
		t.Errorf("Factory should create gRPC transport")
	}

	t.Logf("✅ Transport factory working correctly")
}

func TestRunnerBrokerBackwardsCompatibility(t *testing.T) {
	var output bytes.Buffer

	// Create broker the old way - should use JSON transport by default
	broker := NewRunnerBroker(&output)
	defer broker.Close()

	// Verify it's using JSON transport
	if broker.GetTransport().GetType() != TransportJSON {
		t.Errorf("Default broker should use JSON transport")
	}

	// Test message handler
	var receivedType MessageType
	var receivedPayload json.RawMessage

	broker.RegisterHandler(MessageTypeLog, func(msgType MessageType, payload json.RawMessage) error {
		receivedType = msgType
		receivedPayload = payload
		return nil
	})

	// Send a message
	testPayload := map[string]interface{}{
		"level":   "debug",
		"message": "Backwards compatibility test",
	}

	err := broker.Send(MessageTypeLog, testPayload)
	if err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}

	// Verify output format matches old JSON format
	outputStr := output.String()
	if !strings.Contains(outputStr, `"type":"log"`) {
		t.Errorf("Output should contain JSON message type")
	}

	// Use the variables to avoid linter errors
	_ = receivedType
	_ = receivedPayload

	t.Logf("✅ Backwards compatibility maintained")
	t.Logf("📤 Sent message: %s", strings.TrimSpace(outputStr))
}

func TestRunnerBrokerWithGRPCTransport(t *testing.T) {
	// Create gRPC transport
	grpcTransport, err := NewGRPCTransport("localhost", 50054)
	if err != nil {
		t.Fatalf("Failed to create gRPC transport: %v", err)
	}
	defer grpcTransport.Close()

	// Create broker with gRPC transport
	broker := NewRunnerBrokerWithTransport(grpcTransport)
	defer broker.Close()

	// Verify it's using gRPC transport
	if broker.GetTransport().GetType() != TransportGRPC {
		t.Errorf("Broker should use gRPC transport")
	}

	// Test setting up handlers (even though we can't easily test the full flow)
	broker.RegisterHandler(MessageTypeLog, func(msgType MessageType, payload json.RawMessage) error {
		return nil
	})

	t.Logf("✅ gRPC transport broker created successfully")
}

func TestEnvironmentConfiguration(t *testing.T) {
	// Test default transport type from environment
	transportType := GetTransportTypeFromEnv()
	if transportType != TransportJSON {
		t.Logf("ℹ️  Environment transport type: %s", transportType)
	} else {
		t.Logf("✅ Default transport type: %s", transportType)
	}

	// Test gRPC address from environment
	address, port := GetGRPCAddressFromEnv()
	if address == "localhost" && port == 50051 {
		t.Logf("✅ Default gRPC address: %s:%d", address, port)
	} else {
		t.Logf("ℹ️  Custom gRPC address: %s:%d", address, port)
	}
}

func TestMiddlewareSupport(t *testing.T) {
	var output bytes.Buffer
	transport := NewJSONTransport(&output)

	// Create a simple middleware that modifies messages
	middleware := IPCMiddlewareFunc{
		ProcessOutboundFunc: func(msgType MessageType, payload interface{}) (MessageType, interface{}, error) {
			// Add a timestamp to all outbound messages
			if payloadMap, ok := payload.(map[string]interface{}); ok {
				payloadMap["middleware_timestamp"] = time.Now().Unix()
			}
			return msgType, payload, nil
		},
	}

	transport.AddMiddleware(middleware)

	// Send a message
	err := transport.Send(MessageTypeLog, map[string]interface{}{
		"level":   "info",
		"message": "Middleware test",
	})

	if err != nil {
		t.Fatalf("Failed to send message through middleware: %v", err)
	}

	// Check that middleware added the timestamp
	outputStr := output.String()
	if !strings.Contains(outputStr, "middleware_timestamp") {
		t.Errorf("Middleware should have added timestamp")
	}

	t.Logf("✅ Middleware support working")
	t.Logf("📤 Message with middleware: %s", strings.TrimSpace(outputStr))
}

func TestTypeConversionsInGRPCTransport(t *testing.T) {
	transport, err := NewGRPCTransport("localhost", 50055)
	if err != nil {
		t.Fatalf("Failed to create gRPC transport: %v", err)
	}
	defer transport.Close()

	// Test various message types can be converted to protobuf
	testCases := []struct {
		msgType MessageType
		payload map[string]interface{}
	}{
		{
			MessageTypeLog,
			map[string]interface{}{
				"level":   "error",
				"message": "Test error message",
			},
		},
		{
			MessageTypeStorePut,
			map[string]interface{}{
				"key":   "test_key",
				"value": "test_value",
			},
		},
		{
			MessageTypeStoreDelete,
			map[string]interface{}{
				"key": "key_to_delete",
			},
		},
		{
			MessageTypeFinalStore,
			map[string]interface{}{
				"data": map[string]interface{}{
					"final_key": "final_value",
				},
			},
		},
	}

	for _, tc := range testCases {
		protoMsg, err := transport.convertToProtoMessage(tc.msgType, tc.payload)
		if err != nil {
			t.Errorf("Failed to convert %s message to proto: %v", tc.msgType, err)
			continue
		}

		if protoMsg.Type != convertToProtoMessageType(tc.msgType) {
			t.Errorf("Proto message type mismatch for %s", tc.msgType)
		}

		t.Logf("✅ %s message converts to protobuf correctly", tc.msgType)
	}
}
