package gostage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/davidroman0O/gostage/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCTransport implements IPCTransport using gRPC protocol with type-safe communication
type GRPCTransport struct {
	mu         sync.RWMutex
	address    string
	port       int
	actualPort int // Store the actual port assigned by the system

	// Server-side components (for parent process)
	server         *grpc.Server
	listener       net.Listener
	serverReady    chan struct{}      // Signals when server is ready
	workflowServer *workflowIPCServer // Reference to the workflow server

	// Client-side components (for child process)
	client      proto.WorkflowIPCClient
	conn        *grpc.ClientConn
	clientReady chan struct{} // Signals when client is connected

	// Message handling
	handlers         map[MessageType]MessageHandler
	defaultHandler   MessageHandler
	middleware       []IPCMiddleware
	messageCallbacks []func(MessageType, json.RawMessage) error

	// Communication
	isServer bool
	ctx      context.Context
	cancel   context.CancelFunc

	workflowInitHandler func(*proto.WorkflowDefinition) error
}

// NewGRPCTransport creates a new gRPC-based transport
func NewGRPCTransport(address string, port int) (*GRPCTransport, error) {
	ctx, cancel := context.WithCancel(context.Background())

	return &GRPCTransport{
		address:          address,
		port:             port,
		actualPort:       port, // Initialize with the requested port
		handlers:         make(map[MessageType]MessageHandler),
		middleware:       make([]IPCMiddleware, 0),
		messageCallbacks: make([]func(MessageType, json.RawMessage) error, 0),
		ctx:              ctx,
		cancel:           cancel,
		serverReady:      make(chan struct{}),
		clientReady:      make(chan struct{}),
	}, nil
}

// StartServer starts the gRPC server (for parent process)
func (g *GRPCTransport) StartServer() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.server != nil {
		return fmt.Errorf("server already started")
	}

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", g.address, g.port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	g.listener = lis
	g.server = grpc.NewServer()
	g.isServer = true

	// Store the actual port that was assigned
	if tcpAddr, ok := lis.Addr().(*net.TCPAddr); ok {
		g.actualPort = tcpAddr.Port
	} else {
		g.actualPort = g.port // fallback to original port
	}

	// Register the service implementation
	workflowServer := &workflowIPCServer{transport: g}
	g.workflowServer = workflowServer // Store reference for later access
	proto.RegisterWorkflowIPCServer(g.server, workflowServer)

	// Start serving in a goroutine
	go func(listener net.Listener, server *grpc.Server) {
		// Signal that server is ready to accept connections
		close(g.serverReady)

		if listener != nil && server != nil {
			if err := server.Serve(listener); err != nil {
				// Log error but don't panic
				fmt.Printf("gRPC server error: %v\n", err)
			}
		}
	}(g.listener, g.server)

	return nil
}

// ConnectClient connects as a gRPC client (for child process)
func (g *GRPCTransport) ConnectClient() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.conn != nil {
		return fmt.Errorf("client already connected")
	}

	// Use context with timeout for connection
	ctx, cancel := context.WithTimeout(g.ctx, 30*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		fmt.Sprintf("%s:%d", g.address, g.port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(), // Wait for connection to be established
	)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	g.conn = conn
	g.client = proto.NewWorkflowIPCClient(conn)
	g.isServer = false

	// Signal that client is ready
	close(g.clientReady)

	return nil
}

// Send sends a message through the gRPC transport
func (g *GRPCTransport) Send(msgType MessageType, payload interface{}) error {
	// Process through outbound middleware chain
	g.mu.RLock()
	middleware := make([]IPCMiddleware, len(g.middleware))
	copy(middleware, g.middleware)
	g.mu.RUnlock()

	currentType := msgType
	currentPayload := payload
	var err error

	for _, mw := range middleware {
		currentType, currentPayload, err = mw.ProcessOutbound(currentType, currentPayload)
		if err != nil {
			return fmt.Errorf("IPC middleware error: %w", err)
		}
	}

	// Convert to protobuf message
	protoMsg, err := g.convertToProtoMessage(currentType, currentPayload)
	if err != nil {
		return fmt.Errorf("failed to convert to proto message: %w", err)
	}

	// Send via gRPC client
	if g.client == nil {
		return fmt.Errorf("gRPC client not connected")
	}

	ctx, cancel := context.WithTimeout(g.ctx, 30*time.Second)
	defer cancel()

	ack, err := g.client.SendMessage(ctx, protoMsg)
	if err != nil {
		return fmt.Errorf("gRPC send failed: %w", err)
	}

	if !ack.Success {
		return fmt.Errorf("message rejected: %s", ack.ErrorMessage)
	}

	return nil
}

// convertToProtoMessageType converts a string MessageType to a protobuf MessageType enum
func convertToProtoMessageType(msgType MessageType) proto.MessageType {
	switch msgType {
	case MessageTypeLog:
		return proto.MessageType_MESSAGE_TYPE_LOG
	case MessageTypeStorePut:
		return proto.MessageType_MESSAGE_TYPE_STORE_PUT
	case MessageTypeStoreDelete:
		return proto.MessageType_MESSAGE_TYPE_STORE_DELETE
	case MessageTypeWorkflowStart:
		return proto.MessageType_MESSAGE_TYPE_WORKFLOW_START
	case MessageTypeWorkflowResult:
		return proto.MessageType_MESSAGE_TYPE_WORKFLOW_RESULT
	case MessageTypeFinalStore:
		return proto.MessageType_MESSAGE_TYPE_FINAL_STORE
	default:
		return proto.MessageType_MESSAGE_TYPE_UNSPECIFIED
	}
}

// convertFromProtoMessageType converts a protobuf MessageType enum to a string MessageType
func convertFromProtoMessageType(protoType proto.MessageType) MessageType {
	switch protoType {
	case proto.MessageType_MESSAGE_TYPE_LOG:
		return MessageTypeLog
	case proto.MessageType_MESSAGE_TYPE_STORE_PUT:
		return MessageTypeStorePut
	case proto.MessageType_MESSAGE_TYPE_STORE_DELETE:
		return MessageTypeStoreDelete
	case proto.MessageType_MESSAGE_TYPE_WORKFLOW_START:
		return MessageTypeWorkflowStart
	case proto.MessageType_MESSAGE_TYPE_WORKFLOW_RESULT:
		return MessageTypeWorkflowResult
	case proto.MessageType_MESSAGE_TYPE_FINAL_STORE:
		return MessageTypeFinalStore
	default:
		return MessageType("unknown")
	}
}

// convertToProtoMessage converts a MessageType and payload to a protobuf IPCMessage
func (g *GRPCTransport) convertToProtoMessage(msgType MessageType, payload interface{}) (*proto.IPCMessage, error) {
	msg := &proto.IPCMessage{
		Type:      convertToProtoMessageType(msgType),
		MessageId: fmt.Sprintf("%d", time.Now().UnixNano()),
		Timestamp: time.Now().Unix(),
	}

	// Convert payload to JSON bytes for now (we can make this more type-safe later)
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Set the appropriate payload based on message type
	switch msgType {
	case MessageTypeLog:
		// Parse as log payload
		var logData map[string]interface{}
		if err := json.Unmarshal(payloadBytes, &logData); err != nil {
			return nil, err
		}

		logPayload := &proto.LogPayload{
			Level:     getStringFromMap(logData, "level"),
			Message:   getStringFromMap(logData, "message"),
			Timestamp: time.Now().Unix(),
			Metadata:  make(map[string]string),
		}

		msg.Payload = &proto.IPCMessage_Log{Log: logPayload}

	case MessageTypeStorePut:
		var storeData map[string]interface{}
		if err := json.Unmarshal(payloadBytes, &storeData); err != nil {
			return nil, err
		}

		// Extract the value and marshal it properly
		value := storeData["value"]
		valueBytes, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal store value: %w", err)
		}

		storePutPayload := &proto.StorePutPayload{
			Key:       getStringFromMap(storeData, "key"),
			Value:     valueBytes,
			ValueType: "json",
		}

		msg.Payload = &proto.IPCMessage_StorePut{StorePut: storePutPayload}

	case MessageTypeStoreDelete:
		var storeData map[string]interface{}
		if err := json.Unmarshal(payloadBytes, &storeData); err != nil {
			return nil, err
		}

		storeDeletePayload := &proto.StoreDeletePayload{
			Key: getStringFromMap(storeData, "key"),
		}

		msg.Payload = &proto.IPCMessage_StoreDelete{StoreDelete: storeDeletePayload}

	case MessageTypeFinalStore:
		finalStorePayload := &proto.FinalStorePayload{
			StoreData: make(map[string][]byte),
			TypeInfo:  make(map[string]string),
		}

		// Parse the store data directly from the payload
		var storeMap map[string]interface{}
		if err := json.Unmarshal(payloadBytes, &storeMap); err == nil {
			for k, v := range storeMap {
				if valueBytes, err := json.Marshal(v); err == nil {
					finalStorePayload.StoreData[k] = valueBytes
					finalStorePayload.TypeInfo[k] = "json"
				}
			}
		}

		msg.Payload = &proto.IPCMessage_FinalStore{FinalStore: finalStorePayload}

	default:
		// For unknown message types, we'll need to handle them generically
		return nil, fmt.Errorf("unsupported message type: %v", msgType)
	}

	return msg, nil
}

// Helper function to safely get string from map
func getStringFromMap(m map[string]interface{}, key string) string {
	if val, ok := m[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

// Listen is not needed for gRPC as it uses the server handler pattern
func (g *GRPCTransport) Listen(reader io.Reader) error {
	if g.isServer {
		// For server mode, we don't need to listen to a reader
		// The gRPC server handles incoming connections
		return nil
	}
	return fmt.Errorf("Listen not supported for gRPC client mode")
}

// SetHandler registers a handler for a specific message type
func (g *GRPCTransport) SetHandler(msgType MessageType, handler MessageHandler) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.handlers[msgType] = handler
}

// SetDefaultHandler sets the default handler for unregistered message types
func (g *GRPCTransport) SetDefaultHandler(handler MessageHandler) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.defaultHandler = handler
}

// AddMiddleware adds IPC middleware to the transport
func (g *GRPCTransport) AddMiddleware(middleware ...IPCMiddleware) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.middleware = append(g.middleware, middleware...)
}

// AddMessageCallback adds a callback that will be called for every received message
func (g *GRPCTransport) AddMessageCallback(callback func(MessageType, json.RawMessage) error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.messageCallbacks = append(g.messageCallbacks, callback)
}

// Close closes the gRPC transport and cleans up resources
func (g *GRPCTransport) Close() error {
	g.cancel()

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.server != nil {
		g.server.GracefulStop()
		g.server = nil
	}

	if g.listener != nil {
		g.listener.Close()
		g.listener = nil
	}

	if g.conn != nil {
		g.conn.Close()
		g.conn = nil
	}

	return nil
}

// processIncomingMessage processes a received protobuf message through handlers and middleware
func (g *GRPCTransport) processIncomingMessage(protoMsg *proto.IPCMessage) error {
	// Convert proto message back to our internal format
	msgType := convertFromProtoMessageType(protoMsg.Type)

	// Convert the payload back to JSON for compatibility with existing handlers
	var payload json.RawMessage
	var err error

	switch protoMsg.Payload.(type) {
	case *proto.IPCMessage_Log:
		logPayload := protoMsg.GetLog()
		data := map[string]interface{}{
			"level":   logPayload.Level,
			"message": logPayload.Message,
		}
		payload, err = json.Marshal(data)

	case *proto.IPCMessage_StorePut:
		storePut := protoMsg.GetStorePut()

		// Decode the value from the protobuf
		var value interface{}
		if err := json.Unmarshal(storePut.Value, &value); err != nil {
			return fmt.Errorf("failed to unmarshal store value: %w", err)
		}

		data := map[string]interface{}{
			"key":   storePut.Key,
			"value": value,
		}
		payload, err = json.Marshal(data)

	case *proto.IPCMessage_StoreDelete:
		storeDelete := protoMsg.GetStoreDelete()
		data := map[string]interface{}{
			"key": storeDelete.Key,
		}
		payload, err = json.Marshal(data)

	case *proto.IPCMessage_FinalStore:
		finalStore := protoMsg.GetFinalStore()
		storeData := make(map[string]json.RawMessage)
		for k, v := range finalStore.StoreData {
			storeData[k] = json.RawMessage(v)
		}
		payload, err = json.Marshal(storeData)

	default:
		return fmt.Errorf("unknown payload type in message")
	}

	if err != nil {
		return fmt.Errorf("failed to convert payload: %w", err)
	}

	// Process through inbound middleware chain
	g.mu.RLock()
	middleware := make([]IPCMiddleware, len(g.middleware))
	copy(middleware, g.middleware)
	callbacks := make([]func(MessageType, json.RawMessage) error, len(g.messageCallbacks))
	copy(callbacks, g.messageCallbacks)
	g.mu.RUnlock()

	currentType := msgType
	currentPayload := payload

	for _, mw := range middleware {
		currentType, currentPayload, err = mw.ProcessInbound(currentType, currentPayload)
		if err != nil {
			return fmt.Errorf("IPC middleware inbound error: %w", err)
		}
	}

	// Find and execute handler
	g.mu.RLock()
	handler, exists := g.handlers[currentType]
	if !exists {
		handler = g.defaultHandler
	}
	g.mu.RUnlock()

	if handler != nil {
		if err := handler(currentType, currentPayload); err != nil {
			return fmt.Errorf("handler error: %w", err)
		}
	}

	// Call message callbacks
	for _, callback := range callbacks {
		if err := callback(currentType, currentPayload); err != nil {
			// Log error but don't fail
			fmt.Printf("Message callback error: %v\n", err)
		}
	}

	return nil
}

// workflowIPCServer implements the gRPC server interface
type workflowIPCServer struct {
	proto.UnimplementedWorkflowIPCServer
	transport           *GRPCTransport
	childReadySignals   map[string]chan struct{}             // Track which children are ready
	pendingWorkflowDefs map[string]*proto.WorkflowDefinition // Store workflow definitions for children
	mu                  sync.RWMutex
}

// ChildReady signals that a child process is ready to receive workflow definitions
func (s *workflowIPCServer) ChildReady(ctx context.Context, req *proto.ReadySignal) (*proto.MessageAck, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.childReadySignals == nil {
		s.childReadySignals = make(map[string]chan struct{})
	}

	// Signal that this child is ready
	if readyChan, exists := s.childReadySignals[req.ChildId]; exists {
		close(readyChan)
		delete(s.childReadySignals, req.ChildId)
	}

	return &proto.MessageAck{
		Success:   true,
		MessageId: req.ChildId,
	}, nil
}

// WaitForChildReady waits for a specific child to signal it's ready
func (g *GRPCTransport) WaitForChildReady(childId string, timeout time.Duration) error {
	server := g.getWorkflowServer()
	if server == nil {
		return fmt.Errorf("gRPC server not available")
	}

	server.mu.Lock()
	if server.childReadySignals == nil {
		server.childReadySignals = make(map[string]chan struct{})
	}
	readyChan := make(chan struct{})
	server.childReadySignals[childId] = readyChan
	server.mu.Unlock()

	select {
	case <-readyChan:
		return nil
	case <-time.After(timeout):
		// Clean up the channel
		server.mu.Lock()
		delete(server.childReadySignals, childId)
		server.mu.Unlock()
		return fmt.Errorf("timeout waiting for child %s to be ready", childId)
	}
}

// Helper to get the workflow server
func (g *GRPCTransport) getWorkflowServer() *workflowIPCServer {
	// This is a bit hacky but necessary since we don't have direct access
	// We'll store the server reference when we register it
	return g.workflowServer
}

// InitializeWorkflow signals that a child process is ready to receive workflow definitions
func (g *GRPCTransport) SignalChildReady(ctx context.Context, childId string) error {
	g.mu.RLock()
	client := g.client
	g.mu.RUnlock()

	if client == nil {
		return fmt.Errorf("gRPC client not connected")
	}

	_, err := client.ChildReady(ctx, &proto.ReadySignal{ChildId: childId})
	return err
}

// InitializeWorkflow handles workflow initialization requests from child processes
func (s *workflowIPCServer) InitializeWorkflow(ctx context.Context, req *proto.WorkflowDefinition) (*proto.WorkflowAck, error) {
	// This is actually a request FROM child TO parent asking for workflow definition
	// In our new model, we'll use a different approach
	return &proto.WorkflowAck{
		Success:      false,
		ErrorMessage: "InitializeWorkflow should not be called directly - use RequestWorkflowDefinition",
	}, nil
}

// RequestWorkflowDefinition allows child to request its workflow definition
func (s *workflowIPCServer) RequestWorkflowDefinition(ctx context.Context, req *proto.ReadySignal) (*proto.WorkflowDefinition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Signal that this child is ready
	if s.childReadySignals == nil {
		s.childReadySignals = make(map[string]chan struct{})
	}
	if readyChan, exists := s.childReadySignals[req.ChildId]; exists {
		close(readyChan)
		delete(s.childReadySignals, req.ChildId)
	}

	// Return the pending workflow definition for this child
	if s.pendingWorkflowDefs == nil {
		return nil, fmt.Errorf("no pending workflow definitions")
	}

	workflowDef, exists := s.pendingWorkflowDefs[req.ChildId]
	if !exists {
		return nil, fmt.Errorf("no workflow definition available for child %s", req.ChildId)
	}

	// Remove the definition after sending it
	delete(s.pendingWorkflowDefs, req.ChildId)

	return workflowDef, nil
}

// SetPendingWorkflowDef stores a workflow definition for a specific child
func (g *GRPCTransport) SetPendingWorkflowDef(childId string, def SubWorkflowDef) error {
	server := g.getWorkflowServer()
	if server == nil {
		return fmt.Errorf("gRPC server not available")
	}

	// Convert SubWorkflowDef to protobuf WorkflowDefinition
	defBytes, err := json.Marshal(def)
	if err != nil {
		return fmt.Errorf("failed to serialize workflow definition: %w", err)
	}

	// Convert initial store to protobuf format
	initialStore := make(map[string][]byte)
	if def.InitialStore != nil {
		for k, v := range def.InitialStore {
			valueBytes, err := json.Marshal(v)
			if err != nil {
				return fmt.Errorf("failed to marshal initial store value for key %s: %w", k, err)
			}
			initialStore[k] = valueBytes
		}
	}

	workflowDef := &proto.WorkflowDefinition{
		Id:             def.ID,
		Name:           def.Name,
		DefinitionJson: defBytes,
		InitialStore:   initialStore,
	}

	server.mu.Lock()
	if server.pendingWorkflowDefs == nil {
		server.pendingWorkflowDefs = make(map[string]*proto.WorkflowDefinition)
	}
	server.pendingWorkflowDefs[childId] = workflowDef
	server.mu.Unlock()

	return nil
}

// SendMessage handles incoming gRPC messages
func (s *workflowIPCServer) SendMessage(ctx context.Context, msg *proto.IPCMessage) (*proto.MessageAck, error) {
	err := s.transport.processIncomingMessage(msg)

	ack := &proto.MessageAck{
		Success:   err == nil,
		MessageId: msg.MessageId,
	}

	if err != nil {
		ack.ErrorMessage = err.Error()
	}

	return ack, nil
}

// WaitForServerReady blocks until the gRPC server is ready to accept connections
func (g *GRPCTransport) WaitForServerReady(ctx context.Context) error {
	select {
	case <-g.serverReady:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitForClientReady blocks until the gRPC client is connected and ready
func (g *GRPCTransport) WaitForClientReady(ctx context.Context) error {
	select {
	case <-g.clientReady:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// IsServerReady returns true if the server is ready (non-blocking check)
func (g *GRPCTransport) IsServerReady() bool {
	select {
	case <-g.serverReady:
		return true
	default:
		return false
	}
}

// IsClientReady returns true if the client is connected (non-blocking check)
func (g *GRPCTransport) IsClientReady() bool {
	select {
	case <-g.clientReady:
		return true
	default:
		return false
	}
}

// GetActualPort returns the actual port number assigned by the system
// This is useful when the initial port was 0 (system picks available port)
func (g *GRPCTransport) GetActualPort() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.actualPort
}

// InitializeWorkflow sends a workflow definition to the child process via gRPC
func (g *GRPCTransport) InitializeWorkflow(ctx context.Context, def SubWorkflowDef) error {
	g.mu.RLock()
	client := g.client
	g.mu.RUnlock()

	if client == nil {
		return fmt.Errorf("gRPC client not connected")
	}

	// Convert SubWorkflowDef to protobuf WorkflowDefinition
	defBytes, err := json.Marshal(def)
	if err != nil {
		return fmt.Errorf("failed to serialize workflow definition: %w", err)
	}

	// Convert initial store to protobuf format
	initialStore := make(map[string][]byte)
	if def.InitialStore != nil {
		for k, v := range def.InitialStore {
			valueBytes, err := json.Marshal(v)
			if err != nil {
				return fmt.Errorf("failed to marshal initial store value for key %s: %w", k, err)
			}
			initialStore[k] = valueBytes
		}
	}

	workflowDef := &proto.WorkflowDefinition{
		Id:             def.ID,
		Name:           def.Name,
		DefinitionJson: defBytes,
		InitialStore:   initialStore,
	}

	// Send the workflow definition via gRPC
	ack, err := client.InitializeWorkflow(ctx, workflowDef)
	if err != nil {
		return fmt.Errorf("failed to send workflow definition: %w", err)
	}

	if !ack.Success {
		return fmt.Errorf("workflow initialization rejected: %s", ack.ErrorMessage)
	}

	return nil
}

// SetWorkflowInitHandler sets the handler for workflow initialization (used by child process)
func (g *GRPCTransport) SetWorkflowInitHandler(handler func(*proto.WorkflowDefinition) error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.workflowInitHandler = handler
}

// RequestWorkflowDefinitionFromParent allows child to request workflow definition from parent
func (g *GRPCTransport) RequestWorkflowDefinitionFromParent(ctx context.Context, childId string) (*SubWorkflowDef, error) {
	g.mu.RLock()
	client := g.client
	g.mu.RUnlock()

	if client == nil {
		return nil, fmt.Errorf("gRPC client not connected")
	}

	// Request workflow definition from parent
	workflowDef, err := client.RequestWorkflowDefinition(ctx, &proto.ReadySignal{ChildId: childId})
	if err != nil {
		return nil, fmt.Errorf("failed to request workflow definition: %w", err)
	}

	// Convert protobuf definition back to SubWorkflowDef
	var def SubWorkflowDef
	if err := json.Unmarshal(workflowDef.DefinitionJson, &def); err != nil {
		return nil, fmt.Errorf("failed to unmarshal workflow definition: %w", err)
	}

	// Convert initial store from protobuf format
	if len(workflowDef.InitialStore) > 0 {
		if def.InitialStore == nil {
			def.InitialStore = make(map[string]interface{})
		}
		for k, v := range workflowDef.InitialStore {
			var value interface{}
			if err := json.Unmarshal(v, &value); err != nil {
				return nil, fmt.Errorf("failed to unmarshal initial store value for key %s: %w", k, err)
			}
			def.InitialStore[k] = value
		}
	}

	return &def, nil
}

// GetGRPCAddressFromEnv returns the gRPC address from environment
// Defaults to localhost:50051
func GetGRPCAddressFromEnv() (string, int) {
	address := "localhost"
	port := 50051 // Default port
	return address, port
}
