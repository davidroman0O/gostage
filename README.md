# gostage

[![Go Reference](https://pkg.go.dev/badge/github.com/davidroman0O/gostage.svg)](https://pkg.go.dev/github.com/davidroman0O/gostage)
[![Go Report Card](https://goreportcard.com/badge/github.com/davidroman0O/gostage)](https://goreportcard.com/report/github.com/davidroman0O/gostage)
[![License](https://img.shields.io/github/license/davidroman0O/gostage)](https://github.com/davidroman0O/gostage/blob/main/LICENSE)

gostage is a workflow orchestration and state management library for Go that enables you to build multi-stage stateful workflows with runtime modification capabilities. It provides a framework for organizing complex processes into manageable stages and actions with rich metadata support.

## Overview

gostage provides a structured approach to workflow management with these core components:

- **Workflows** - The top-level container representing an entire process
- **Stages** - Sequential phases within a workflow, each containing multiple actions 
- **Actions** - Individual units of work that implement specific tasks
- **State Store** - A type-safe key-value store for workflow data
- **Metadata** - Rich tagging and property system for organization and querying

## Key Features

- **Sequential Execution** - Workflows execute stages and actions in defined order
- **Dynamic Modification** - Add or modify workflow components during execution
- **Tag-Based Organization** - Categorize and filter components for better organization
- **Type-Safe Store** - Store and retrieve data with type checking and TTL support
- **Conditional Execution** - Enable/disable specific components at runtime
- **Rich Metadata** - Associate tags and properties with workflow components
- **Serializable State** - Store workflow state during and between executions
- **Extensible Middleware** - Add cross-cutting concerns like logging, error handling, and retry logic
- **Hierarchical Middleware** - Customize workflow behavior at multiple levels

## Installation

```bash
go get github.com/davidroman0O/gostage
```

## Basic Usage

Here's a simple example of creating and executing a workflow:

```go
package main

import (
	"context"
	"fmt"
	
	"github.com/davidroman0O/gostage"
)

// Define a custom action by embedding BaseAction
type GreetingAction struct {
	gostage.BaseAction
}

// Implement the Execute method required by the Action interface
func (a GreetingAction) Execute(ctx *gostage.ActionContext) error {
	name, err := gostage.ContextGetOrDefault(ctx, "user.name", "World")
	if err != nil {
		return err
	}
	
	ctx.Logger.Info("Hello, %s!", name)
	return nil
}

func main() {
	// Create a new workflow
	wf := gostage.NewWorkflow(
		"hello-world",
		"Hello World Workflow",
		"A simple introductory workflow",
	)
	
	// Create a stage
	stage := gostage.NewStage(
		"greeting",
		"Greeting Stage",
		"Demonstrates a simple greeting",
	)
	
	// Add actions to the stage
	stage.AddAction(&GreetingAction{
		BaseAction: gostage.NewBaseAction("greet", "Greeting Action"),
	})
	
	// Add the stage to the workflow
	wf.AddStage(stage)
	
	// Set up a logger
	logger := gostage.NewDefaultLogger()
	
	// Create a runner
	runner := gostage.NewRunner()

	// Add middleware for logging, error handling, etc.
	runner.Use(gostage.LoggingMiddleware())

	// Execute the workflow
	if err := runner.Execute(context.Background(), wf, logger); err != nil {
		fmt.Printf("Error executing workflow: %v\n", err)
		return
	}
	
	fmt.Println("Workflow completed successfully!")
}
```

## Core Components

### Action Interface

Actions are the building blocks of a workflow:

```go
type Action interface {
	// Name returns the action's name
	Name() string

	// Description returns a human-readable description
	Description() string

	// Tags returns the action's tags for organization and filtering
	Tags() []string

	// Execute performs the action's work
	Execute(ctx *ActionContext) error
}
```

### Stages

Stages are containers for actions that execute sequentially:

```go
// Create a stage with tags
stage := gostage.NewStageWithTags(
    "validation", 
    "Order Validation", 
    "Validates incoming orders", 
    []string{"critical", "input"},
)

// Add actions to the stage
stage.AddAction(myAction)
```

### Workflow

Workflows manage the execution of stages:

```go
// Create a workflow
wf := gostage.NewWorkflow(
    "process-orders", 
    "Order Processing", 
    "Handles end-to-end order processing",
)

// Add stages to the workflow
wf.AddStage(stage1)
wf.AddStage(stage2)
```

### Runner

Runners execute workflows and can be customized with middleware:

```go
// Create a runner
runner := gostage.NewRunner()

// Add middleware for logging, error handling, etc.
runner.Use(gostage.LoggingMiddleware())

// Execute the workflow
runner.Execute(context.Background(), wf, logger)
```

Runners can also be extended to create domain-specific workflow executors (see the "Extending the Runner" section below).

### Middleware

Middleware provides a powerful way to intercept and enhance workflow execution with cross-cutting concerns. Each middleware wraps the execution flow, allowing you to perform actions before and after workflow execution.

```go
// Create a runner with multiple middleware components
runner := gostage.NewRunner(
    gostage.WithMiddleware(
        gostage.LoggingMiddleware(),                       // Built-in logging
        ErrorHandlingMiddleware([]string{"non-critical"}), // Custom error handling
        TimingMiddleware(),                                // Performance monitoring
        RetryMiddleware(3, 100*time.Millisecond),          // Automatic retries
    ),
)
```

Key characteristics of middleware:

- **Pre/Post Execution** - Run code before and after workflow execution
- **Error Handling** - Catch, transform, or recover from errors
- **Context Modification** - Add values to or modify the execution context
- **Chain Execution** - Multiple middleware components work together in a chain

#### Creating Custom Middleware

Creating your own middleware is straightforward:

```go
func MyCustomMiddleware() gostage.Middleware {
    return func(next gostage.RunnerFunc) gostage.RunnerFunc {
        return func(ctx context.Context, wf *gostage.Workflow, logger gostage.Logger) error {
            // Pre-execution logic
            logger.Info("Starting workflow execution with custom middleware")
            
            // Execute the next middleware in the chain (or the workflow itself)
            err := next(ctx, wf, logger)
            
            // Post-execution logic
            logger.Info("Workflow execution completed with result: %v", err == nil)
            
            // Optionally transform or handle the error
            return err
        }
    }
}
```

#### Common Middleware Patterns

The library includes examples of several middleware patterns:

1. **Error Handling Middleware** - Catch and recover from specific errors
2. **Retry Middleware** - Automatically retry workflows that fail
3. **Timing Middleware** - Measure and record execution time
4. **Validation Middleware** - Ensure workflows meet certain criteria
5. **State Injection Middleware** - Add initial state to workflows
6. **Tracing Middleware** - Add distributed tracing capabilities
7. **Audit Middleware** - Record workflow execution for compliance

See the `examples/middleware` directory for complete implementations of these patterns.

#### Middleware Order

The order of middleware registration is important. Middleware is applied in reverse order, so the last middleware registered is the first to execute and the closest to the actual workflow execution.

```go
runner.Use(
    middlewareA, // Applied third (outer layer)
    middlewareB, // Applied second (middle layer)
    middlewareC, // Applied first (inner layer, closest to workflow)
)
```

This structure allows outer middleware to take action based on the results of inner middleware.

### State Management

gostage includes a key-value store with type safety:

```go
// Store data
ctx.Store().Put("order.id", "ORD-12345")

// Retrieve data with type safety
orderId, err := store.Get[string](ctx.Store(), "order.id")

// Store with TTL (time-to-live)
ctx.Store().PutWithTTL("session.token", token, 24*time.Hour)

// Store with metadata
metadata := store.NewMetadata()
metadata.AddTag("sensitive")
metadata.SetProperty("source", "external-api")
ctx.Store().PutWithMetadata("customer.data", customerData, metadata)
```

## Advanced Features

### Dynamic Action Generation

Actions can dynamically generate additional actions during execution:

```go
func (a DynamicAction) Execute(ctx *gostage.ActionContext) error {
    // Create a new action dynamically
    newAction := &CustomAction{
        BaseAction: gostage.NewBaseAction("dynamic-action", "Dynamically Created Action"),
    }
    
    // Add it to be executed after this action
    ctx.AddDynamicAction(newAction)
    
    return nil
}
```

### Dynamic Stage Generation

Stages can be created dynamically during workflow execution:

```go
func (a StageGeneratorAction) Execute(ctx *gostage.ActionContext) error {
    // Create a new stage dynamically
    newStage := gostage.NewStage(
        "dynamic-stage", 
        "Dynamic Stage", 
        "Created based on runtime conditions",
    )
    
    // Add actions to the stage
    newStage.AddAction(newAction)
    
    // Add it to be executed after the current stage
    ctx.AddDynamicStage(newStage)
    
    return nil
}
```

### Conditional Execution

Actions and stages can be conditionally enabled or disabled:

```go
// Disable a specific action
ctx.DisableAction("resource-intensive-action")

// Disable actions by tag
ctx.DisableActionsByTag("optional")

// Disable a specific stage
ctx.DisableStage("cleanup-stage")

// Enable/disable based on conditions
if !ctx.Store().HasTag("important-resource", "protected") {
    ctx.EnableStage("cleanup-stage")
}
```

### Filtering and Finding Components

Find workflow components using advanced filtering:

```go
// Find actions by tag
criticalActions := ctx.FindActionsByTag("critical")

// Find actions by multiple tags
backupActions := ctx.FindActionsByTags([]string{"backup", "database"})

// Find stages by description substring
reportStages := ctx.FindStagesByDescription("report")

// Find actions by type
uploadActions := ctx.FindActionsByType((*UploadAction)(nil))

// Custom filtering
complexActions := ctx.FilterActions(func(a gostage.Action) bool {
    return strings.Contains(a.Description(), "complex")
})
```

### Extending the Runner

The Runner can be extended to create domain-specific workflow execution environments:

```go
// Create a custom runner by embedding the base Runner
type ExtendedRunner struct {
    // Embed the base runner
    *gostage.Runner

    // Add domain-specific components
    configProvider   ConfigProvider
    resourceManager  ResourceManager
    
    // Add custom settings
    defaultEnvironment string
    setupTimeout       time.Duration
}

// Override Execute to add custom preparation logic
func (r *ExtendedRunner) Execute(ctx context.Context, wf *Workflow, logger Logger) error {
    // Add preparation logic
    if err := r.prepareWorkflow(wf); err != nil {
        return err
    }
    
    // Call the base implementation
    return r.Runner.Execute(ctx, wf, logger)
}
```

This pattern is helpful when you need to:

1. **Provide domain-specific initialization** - Set up resources, load configuration
2. **Share common resources** - Make services, clients, or tools available to all actions
3. **Create a specialized execution environment** - Add middleware specific to your domain
4. **Simplify workflow creation** - Pre-configure workflows with standard components

The `examples/extended_runner` directory shows a complete example of this pattern, including:
- How to properly store and retrieve domain objects in the workflow store
- How to provide helper functions for accessing typed resources
- How to create a fluent configuration API for your extended runner

## Use Cases

gostage is well-suited for various workflow scenarios:

- **ETL Processes** - Define data extraction, transformation and loading pipelines
- **Deployment Pipelines** - Create sequential deployment steps with conditional execution
- **Business Workflows** - Model complex business processes with state tracking
- **Resource Provisioning** - Set up resource discovery and provisioning sequences
- **Data Processing** - Orchestrate complex data operations with state management
- **Error-Tolerant Flows** - Build resilient workflows with retry logic and error recovery
- **Audited Processes** - Implement compliance requirements with audit trails and validation checks

## Examples

The repository includes several examples demonstrating different features:

- Basic workflow creation and execution
- Dynamic stage generation
- File operations workflow
- Action wrapping patterns
- Conditional execution with enable/disable
- Extended Runner pattern for domain-specific workflows
- Middleware patterns for cross-cutting concerns
- Error handling and recovery strategies

Check the `examples/` directory for complete examples.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## API Changes

### Store Access in ActionContext

The `ActionContext` no longer has a direct `Store` field. Instead, it provides a
`Store()` method that returns the workflow's store. This ensures that all actions operate on the same
store instance and clarifies the ownership relationship.

Before:
```go
func (a *MyAction) Execute(ctx *ActionContext) error {
    // Direct field access
    ctx.Store.Put("key", value)
    
    data, err := store.Get[MyType](ctx.Store, "other-key")
    // ...
}
```

After:
```go
func (a *MyAction) Execute(ctx *ActionContext) error {
    // Method call to get the store
    ctx.Store().Put("key", value)
    
    data, err := store.Get[MyType](ctx.Store(), "other-key")
    // ...
}
```

### Stage Initial Store

The `Stage` no longer exposes the `InitialStore` field directly. Instead, it provides
methods for interacting with the initial store data.

Before:
```go
stage := NewStage("my-stage", "My Stage", "Description")
stage.InitialStore.Put("key", value)
```

After:
```go
stage := NewStage("my-stage", "My Stage", "Description")
stage.SetInitialData("key", value)
```

## Core Components

1. **Workflows** - The top-level container representing an entire process
2. **Stages** - Sequential phases within a workflow, each containing multiple actions
3. **Actions** - Individual units of work that implement specific tasks
4. **State Store** - A type-safe key-value store for workflow data
5. **Middleware** - Customizable hooks that wrap execution at different levels

## Features

- Sequential execution of stages and actions
- Dynamic modification of workflows during execution
- Tag-based organization and filtering
- Type-safe state storage with support for any Go type
- Conditional execution based on runtime state
- Rich metadata for traceability and organization
- Serializable workflow state for persistence
- Hierarchical middleware system

## Middleware System

GoStage provides a powerful hierarchical middleware system that allows you to customize behavior at different levels of execution:

1. **Runner Middleware**: Wraps the entire workflow execution
2. **Workflow Middleware**: Wraps individual stage executions
3. **Stage Middleware**: Wraps all actions within a stage

### Middleware Execution Flow

The execution flow with middleware follows a nested pattern:

```
Runner Middleware (start)
  Workflow (start)
    Workflow Middleware for Stage 1 (start)
      Stage 1 Middleware (start)
        Actions in Stage 1
      Stage 1 Middleware (end)
    Workflow Middleware for Stage 1 (end)
    
    Workflow Middleware for Stage 2 (start)
      Stage 2 Middleware (start)
        Actions in Stage 2
      Stage 2 Middleware (end)
    Workflow Middleware for Stage 2 (end)
  Workflow (end)
Runner Middleware (end)
```

### Using Runner Middleware

Runner middleware wraps the execution of an entire workflow:

```go
runner := gostage.NewRunner()

// Add logging middleware
runner.Use(func(next gostage.RunnerFunc) gostage.RunnerFunc {
    return func(ctx context.Context, w *gostage.Workflow, logger gostage.Logger) error {
        logger.Info("Starting workflow: %s", w.Name)
        
        err := next(ctx, w, logger)
        
        logger.Info("Completed workflow: %s", w.Name)
        return err
    }
})

// Execute the workflow
runner.Execute(context.Background(), workflow, logger)
```

### Using Workflow Middleware

Workflow middleware wraps the execution of each stage within a workflow:

```go
workflow := gostage.NewWorkflow("example", "Example Workflow", "A workflow with middleware")

// Add stage notification middleware
workflow.Use(func(next gostage.WorkflowStageRunnerFunc) gostage.WorkflowStageRunnerFunc {
    return func(ctx context.Context, s *gostage.Stage, w *gostage.Workflow, logger gostage.Logger) error {
        logger.Info("Starting stage: %s", s.Name)
        
        err := next(ctx, s, w, logger)
        
        logger.Info("Completed stage: %s", s.Name)
        return err
    }
})

// Add stages and actions...
```

### Using Stage Middleware

Stage middleware wraps the execution of all actions within a stage:

```go
stage := gostage.NewStage("container-stage", "Container Stage", "A stage that runs in a container")

// Add container middleware
stage.Use(func(next gostage.StageRunnerFunc) gostage.StageRunnerFunc {
    return func(ctx context.Context, s *gostage.Stage, w *gostage.Workflow, logger gostage.Logger) error {
        // Start container
        logger.Info("Starting container for stage: %s", s.Name)
        
        // Execute all actions in the container
        err := next(ctx, s, w, logger)
        
        // Stop container (even if there was an error)
        logger.Info("Stopping container for stage: %s", s.Name)
        
        return err
    }
})

// Add actions that will run in the container...
```

### Built-in Middleware Functions

The middleware system allows you to create various utility middleware functions. Here are examples of middleware you could build with the system:

#### Example Runner Middleware

```go
// Example logging middleware for runners
func LoggingMiddleware() gostage.Middleware {
    return func(next gostage.RunnerFunc) gostage.RunnerFunc {
        return func(ctx context.Context, w *gostage.Workflow, logger gostage.Logger) error {
            start := time.Now()
            logger.Info("Starting workflow: %s", w.Name)
            
            err := next(ctx, w, logger)
            
            elapsed := time.Since(start)
            logger.Info("Completed workflow: %s (in %v)", w.Name, elapsed)
            return err
        }
    }
}

// Example time limit middleware
func TimeLimitMiddleware(duration time.Duration) gostage.Middleware {
    return func(next gostage.RunnerFunc) gostage.RunnerFunc {
        return func(ctx context.Context, w *gostage.Workflow, logger gostage.Logger) error {
            ctx, cancel := context.WithTimeout(ctx, duration)
            defer cancel()
            
            return next(ctx, w, logger)
        }
    }
}
```

#### Example Workflow Middleware

```go
// Example stage notification middleware
func StageNotificationMiddleware(beforeFn, afterFn func(stageName string)) gostage.WorkflowMiddleware {
    return func(next gostage.WorkflowStageRunnerFunc) gostage.WorkflowStageRunnerFunc {
        return func(ctx context.Context, s *gostage.Stage, w *gostage.Workflow, logger gostage.Logger) error {
            if beforeFn != nil {
                beforeFn(s.Name)
            }
            
            err := next(ctx, s, w, logger)
            
            if afterFn != nil {
                afterFn(s.Name)
            }
            
            return err
        }
    }
}
```

#### Example Stage Middleware

```go
// Example container middleware for stages
func ContainerStageMiddleware(image, name string) gostage.StageMiddleware {
    return func(next gostage.StageRunnerFunc) gostage.StageRunnerFunc {
        return func(ctx context.Context, s *gostage.Stage, w *gostage.Workflow, logger gostage.Logger) error {
            // Start container (pseudocode)
            logger.Info("Starting container %s with image %s", name, image)
            
            // Run all actions in the container
            err := next(ctx, s, w, logger)
            
            // Always stop container
            logger.Info("Stopping container %s", name)
            
            return err
        }
    }
}
```

## More Examples

See the [examples](./examples) directory for more usage examples.

## License

[MIT License](LICENSE) 