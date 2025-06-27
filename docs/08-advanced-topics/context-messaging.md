# Context Messaging

Context messaging provides rich metadata about message sources and enables targeted message handling in gostage. Know exactly where messages come from (workflow, stage, action, process) and route messages to specific handlers.

## Overview

- **Context Metadata**: Automatic workflow/stage/action/process information
- **Targeted Handlers**: Register handlers for specific workflows, stages, or actions
- **Global Handlers**: Receive all messages with rich context
- **Session Tracking**: Unique session IDs and sequence numbers for message ordering
- **Legacy Support**: Existing code works unchanged

## Works With or Without gRPC

**Context messaging works for both normal and spawned workflows:**

### Normal Workflows (No gRPC Required)
```go
// Create a regular runner - no gRPC setup needed
runner := NewRunner()

// Register context handlers - works immediately
runner.Broker.RegisterHandlerWithContext(MessageTypeStorePut,
    func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
        fmt.Printf("Message from %s->%s->%s\n", 
            context.WorkflowID, context.StageID, context.ActionName)
        return nil
    })

// Execute normal workflow - messages handled locally
err := runner.Execute(ctx, workflow, logger)
```

### Spawned Workflows (Uses gRPC)
```go
// Create runner with gRPC transport for spawning
runner := NewRunner(WithGRPCTransport("localhost", 50080))

// Same API - register context handlers
runner.Broker.RegisterHandlerWithContext(MessageTypeStorePut,
    func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
        fmt.Printf("Message from %s->%s->%s (PID: %d)\n", 
            context.WorkflowID, context.StageID, context.ActionName, context.ProcessID)
        return nil
    })

// Spawn workflow - messages sent via gRPC
err := runner.Spawn(ctx, workflowDef)
```

**Same `ctx.Send()` API in both cases** - the difference is handled automatically under the hood.

## Basic Usage

### Same `ctx.Send` - Now with Context

Your existing `ctx.Send` calls work exactly the same, but now automatically include rich context metadata:

```go
func (a *MyAction) Execute(ctx *ActionContext) error {
    // Same as always - but now includes context metadata automatically
    ctx.Send(MessageTypeStorePut, map[string]interface{}{
        "key":   "processing_status", 
        "value": "completed",
    })
    return nil
}
```

### Handler Registration

Context messaging provides multiple levels of handler registration:

```go
// Works with both NewRunner() and NewRunner(WithGRPCTransport(...))
runner := NewRunner(WithGRPCTransport("localhost", 50080))

// 1. Legacy handler (no context metadata)
runner.Broker.RegisterHandler(MessageTypeStorePut, func(msgType MessageType, payload json.RawMessage) error {
    // Handle message without context
    return nil
})

// 2. Global handler with context metadata  
runner.Broker.RegisterHandlerWithContext(MessageTypeStorePut, 
    func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
        fmt.Printf("Message from %s->%s->%s (PID: %d)\n", 
            context.WorkflowID, context.StageID, context.ActionName, context.ProcessID)
        return nil
    })

// 3. Workflow-specific handler
runner.Broker.RegisterWorkflowHandler(MessageTypeStorePut, "my-workflow",
    func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
        // Only receives messages from "my-workflow"
        return nil
    })

// 4. Stage-specific handler  
runner.Broker.RegisterStageHandler(MessageTypeStorePut, "my-workflow", "processing-stage",
    func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
        // Only receives messages from the "processing-stage" in "my-workflow"
        return nil
    })

// 5. Action-specific handler
runner.Broker.RegisterActionHandler(MessageTypeStorePut, "my-workflow", "stage-1", "data-processor",
    func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
        // Only receives messages from the "data-processor" action
        return nil
    })
```

## Context Metadata

```go
type MessageContext struct {
    WorkflowID     string  // ID of the workflow that sent this message
    StageID        string  // ID of the stage that sent this message  
    ActionName     string  // Name of the action that sent this message
    ProcessID      int32   // PID of the process that sent this message
    IsChildProcess bool    // Whether this came from a spawned child process
    ActionIndex    int32   // Index of action within stage (0-based)
    IsLastAction   bool    // Whether this is the last action in the stage
    SessionID      string  // Unique session identifier for this workflow run
    SequenceNumber int64   // Incremental sequence number for this session
}
```

### Using Context Information

```go
runner.Broker.RegisterHandlerWithContext(MessageTypeStorePut,
    func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
        // Process identification
        processInfo := "PARENT"
        if context.IsChildProcess {
            processInfo = fmt.Sprintf("CHILD:%d", context.ProcessID)
        }
        
        // Message tracing
        fmt.Printf("[%s] %s->%s->%s (Seq: %d, Session: %s)\n",
            processInfo, context.WorkflowID, context.StageID, 
            context.ActionName, context.SequenceNumber, context.SessionID)
        
        // Action position awareness
        if context.IsLastAction {
            fmt.Println("This is the last action in the stage")
        }
        
        return nil
    })
```

## Handler Priority

When multiple handlers are registered for the same message type, they are called in this order:

1. **Action-specific handler** (most specific)
2. **Stage-specific handler**
3. **Workflow-specific handler** 
4. **Global context handler**
5. **Legacy handler** (receives message without context)

All applicable handlers will be called - both context handlers and legacy handlers.

## Use Cases

### Workflow-Specific Error Handling

```go
runner.Broker.RegisterWorkflowHandler(MessageTypeLog, "critical-workflow",
    func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
        var logData map[string]string
        json.Unmarshal(payload, &logData)
        
        if logData["level"] == "ERROR" {
            // Send alert for critical workflow errors
            alertSystem.NotifyCriticalError(context.WorkflowID, context.ActionName, logData["message"])
        }
        
        return nil
    })
```

### Performance Monitoring by Stage

```go
runner.Broker.RegisterStageHandler(MessageTypeStorePut, "data-pipeline", "processing-stage",
    func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
        // Collect metrics specifically for the processing stage
        metrics.IncrementStageActivity("data-pipeline", "processing-stage")
        
        if context.IsLastAction {
            metrics.RecordStageCompletion("data-pipeline", "processing-stage") 
        }
        
        return nil
    })
```

### Process-Specific Message Routing

```go
runner.Broker.RegisterHandlerWithContext(MessageTypeStorePut,
    func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
        if context.IsChildProcess {
            // Route child process messages to distributed storage
            distributedStore.Store(context.ProcessID, payload)
        } else {
            // Handle parent process messages locally
            localStore.Store(payload)
        }
        
        return nil
    })
```

### Debug Logging with Full Context

```go
runner.Broker.RegisterHandlerWithContext(MessageTypeLog,
    func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
        var logData map[string]string
        json.Unmarshal(payload, &logData)
        
        processType := "PARENT"
        if context.IsChildProcess {
            processType = fmt.Sprintf("CHILD:%d", context.ProcessID)
        }
        
        logger.Printf("[%s] [%s] %s->%s->%s: %s", 
            processType, logData["level"], context.WorkflowID, 
            context.StageID, context.ActionName, logData["message"])
        
        return nil
    })
```

## Advanced Features

### Custom Context Override

Override context metadata for specific messages:

```go
func (a *MyAction) Execute(ctx *ActionContext) error {
    // Send with custom context overrides
    customContext := map[string]interface{}{
        "workflow_id": "special-tracking-id",
        "stage_id":    "custom-stage",
    }
    
    ctx.SendWithCustomContext(MessageTypeStorePut, payload, customContext)
    return nil
}
```

### Session and Sequence Tracking

Use session IDs and sequence numbers for advanced workflow tracking:

```go
runner.Broker.RegisterHandlerWithContext(MessageTypeStorePut,
    func(msgType MessageType, payload json.RawMessage, context MessageContext) error {
        // Track messages by session
        sessionTracker.RecordMessage(context.SessionID, context.SequenceNumber, payload)
        
        // Detect out-of-order messages
        if !sessionTracker.IsInOrder(context.SessionID, context.SequenceNumber) {
            logger.Warn("Out-of-order message detected in session %s", context.SessionID)
        }
        
        return nil
    })
```

## Complete Example

### Spawned Workflow Example (with gRPC)

```go
package main

import (
    "context"
    "encoding/json" 
    "fmt"
    
    "github.com/davidroman0O/gostage"
)

func main() {
    runner := gostage.NewRunner(gostage.WithGRPCTransport("localhost", 50080))
    
    // Global context handler - receives all messages with context
    runner.Broker.RegisterHandlerWithContext(gostage.MessageTypeStorePut,
        func(msgType gostage.MessageType, payload json.RawMessage, context gostage.MessageContext) error {
            var data map[string]interface{}
            json.Unmarshal(payload, &data)
            
            fmt.Printf("📨 [GLOBAL] %s: %v from %s->%s->%s (PID:%d, Seq:%d)\n",
                data["key"], data["value"], context.WorkflowID, context.StageID, 
                context.ActionName, context.ProcessID, context.SequenceNumber)
            return nil
        })
    
    // Workflow-specific handler
    runner.Broker.RegisterWorkflowHandler(gostage.MessageTypeStorePut, "data-pipeline",
        func(msgType gostage.MessageType, payload json.RawMessage, context gostage.MessageContext) error {
            fmt.Printf("🎯 [PIPELINE] Processing data-pipeline message from %s\n", context.ActionName)
            return nil
        })
    
    // Stage-specific handler  
    runner.Broker.RegisterStageHandler(gostage.MessageTypeStorePut, "data-pipeline", "validation",
        func(msgType gostage.MessageType, payload json.RawMessage, context gostage.MessageContext) error {
            fmt.Printf("🔍 [VALIDATION] Handling validation stage message\n")
            return nil
        })
    
    // Execute workflow...
    workflowDef := gostage.SubWorkflowDef{
        ID: "data-pipeline",
        Name: "Data Processing Pipeline",
        Stages: []gostage.StageDef{
            {
                ID: "validation", 
                Name: "Data Validation",
                Actions: []gostage.ActionDef{{ID: "validate-data"}},
            },
        },
    }
    
    err := runner.Spawn(context.Background(), workflowDef)
    if err != nil {
        fmt.Printf("❌ Workflow failed: %v\n", err)
    }
    
    runner.Broker.Close()
}
```

### Normal Workflow Example (no gRPC)

```go
package main

import (
    "context"
    "encoding/json" 
    "fmt"
    
    "github.com/davidroman0O/gostage"
)

func main() {
    // No gRPC setup needed
    runner := gostage.NewRunner()
    
    // Register context handlers - same API
    runner.Broker.RegisterHandlerWithContext(gostage.MessageTypeStorePut,
        func(msgType gostage.MessageType, payload json.RawMessage, context gostage.MessageContext) error {
            var data map[string]interface{}
            json.Unmarshal(payload, &data)
            
            fmt.Printf("📨 [LOCAL] %s: %v from %s->%s->%s\n",
                data["key"], data["value"], context.WorkflowID, context.StageID, context.ActionName)
            return nil
        })
    
    // Create and execute normal workflow
    workflow := gostage.NewWorkflow("local-workflow", "Local Workflow", "1.0.0")
    stage := gostage.NewStage("processing", "Processing Stage", "Process data")
    
    // Add your actions to the stage here...
    
    workflow.AddStage(stage)
    
    err := runner.Execute(context.Background(), workflow, logger)
    if err != nil {
        fmt.Printf("❌ Workflow failed: %v\n", err)
    }
}
```

## Performance Considerations

- **Context Overhead**: Minimal - context metadata adds ~100 bytes per message
- **Handler Efficiency**: Context handlers are only called for relevant messages  
- **Memory Usage**: Session tracking uses minimal memory per workflow execution
- **Normal Workflows**: No network overhead - messages handled locally in-process
- **Spawned Workflows**: gRPC efficiently serializes the context protobuf messages over network

## Best Practices

1. **Use Appropriate Granularity**: Register handlers at the most specific level needed
2. **Leverage Context**: Use context metadata for filtering and routing decisions
3. **Monitor Sequence Numbers**: Detect message ordering issues or lost messages
4. **Handle Both Types**: Support both legacy and context handlers 
5. **Session Correlation**: Use session IDs for end-to-end workflow tracking
6. **Avoid Over-Filtering**: Don't create overly specific handlers unless necessary

Context messaging provides powerful foundations for building sophisticated, observable, and debuggable workflows. 