package types

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
