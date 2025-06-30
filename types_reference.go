package gostage

/*
This file serves as a reference for all the types that should be moved to types.go.
It doesn't contain any actual code but lists all the types that are used as interfaces,
data transfer objects, or for holding data without methods.

Types to move to types.go:

From communication.go:
- MessageType (string)
- Message struct
- MessageHandler func
- ContextMessageHandler func
- MessageContext struct
- HandlerRegistration struct
- IPCMiddleware interface
- IPCHandler func
- SpawnMiddleware interface
- IPCMiddlewareFunc struct (with methods)
- SpawnMiddlewareFunc struct (with methods)

From action_registry.go:
- ActionFactory func

From action.go:
- Action interface
- ActionRunnerFunc func
- ActionMiddleware func
- ActionState struct
- StageState struct

From logger.go:
- Logger interface

From stage.go:
- StageInfo struct
- StageRunnerFunc func
- StageMiddleware func

From workflow.go:
- WorkflowStageRunnerFunc func
- WorkflowMiddleware func
- WorkflowInfo struct
- MergeStrategy int

From runner.go:
- RunnerFunc func
- Middleware func
- RunOptions struct
- RunResult struct
- RunnerOption func
- SpawnResult struct

From subworkflow.go:
- ActionDef struct
- StageDef struct
- SubWorkflowDef struct

To properly move these types, we need to:
1. Ensure all imports are correctly set up in types.go
2. Copy the type definitions and their documentation
3. Remove the original type definitions from their source files
4. Fix any circular dependencies that might arise
*/
