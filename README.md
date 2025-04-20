# GoState

[![Go Reference](https://pkg.go.dev/badge/github.com/davidroman0O/gostate.svg)](https://pkg.go.dev/github.com/davidroman0O/gostate)
[![Go Report Card](https://goreportcard.com/badge/github.com/davidroman0O/gostate)](https://goreportcard.com/report/github.com/davidroman0O/gostate)
[![License](https://img.shields.io/github/license/davidroman0O/gostate)](https://github.com/davidroman0O/gostate/blob/main/LICENSE)

GoState is a workflow orchestration and state management library for Go that enables you to build multi-stage stateful workflows with runtime modification capabilities. It provides a framework for organizing complex processes into manageable stages and actions with rich metadata support.

## Overview

GoState provides a structured approach to workflow management with these core components:

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

## Installation

```bash
go get github.com/davidroman0O/gostate
```

## Basic Usage

Here's a simple example of creating and executing a workflow:

```go
package main

import (
	"context"
	"fmt"
	
	"github.com/davidroman0O/gostate"
)

// Define a custom action by embedding BaseAction
type GreetingAction struct {
	gostate.BaseAction
}

// Implement the Execute method required by the Action interface
func (a GreetingAction) Execute(ctx *gostate.ActionContext) error {
	name, err := gostate.ContextGetOrDefault(ctx, "user.name", "World")
	if err != nil {
		return err
	}
	
	ctx.Logger.Info("Hello, %s!", name)
	return nil
}

func main() {
	// Create a new workflow
	wf := gostate.NewWorkflow(
		"hello-world",
		"Hello World Workflow",
		"A simple introductory workflow",
	)
	
	// Create a stage
	stage := gostate.NewStage(
		"greeting",
		"Greeting Stage",
		"Demonstrates a simple greeting",
	)
	
	// Add actions to the stage
	stage.AddAction(&GreetingAction{
		BaseAction: gostate.NewBaseAction("greet", "Greeting Action"),
	})
	
	// Add the stage to the workflow
	wf.AddStage(stage)
	
	// Set up a logger
	logger := gostate.NewDefaultLogger()
	
	// Execute the workflow
	if err := wf.Execute(context.Background(), logger); err != nil {
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
stage := gostate.NewStageWithTags(
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
wf := gostate.NewWorkflow(
    "process-orders", 
    "Order Processing", 
    "Handles end-to-end order processing",
)

// Add stages to the workflow
wf.AddStage(stage1)
wf.AddStage(stage2)

// Execute the workflow
wf.Execute(context.Background(), logger)
```

### State Management

GoState includes a key-value store with type safety:

```go
// Store data
ctx.Store.Put("order.id", "ORD-12345")

// Retrieve data with type safety
orderId, err := store.Get[string](ctx.Store, "order.id")

// Store with TTL (time-to-live)
ctx.Store.PutWithTTL("session.token", token, 24*time.Hour)

// Store with metadata
metadata := store.NewMetadata()
metadata.AddTag("sensitive")
metadata.SetProperty("source", "external-api")
ctx.Store.PutWithMetadata("customer.data", customerData, metadata)
```

## Advanced Features

### Dynamic Action Generation

Actions can dynamically generate additional actions during execution:

```go
func (a DynamicAction) Execute(ctx *gostate.ActionContext) error {
    // Create a new action dynamically
    newAction := &CustomAction{
        BaseAction: gostate.NewBaseAction("dynamic-action", "Dynamically Created Action"),
    }
    
    // Add it to be executed after this action
    ctx.AddDynamicAction(newAction)
    
    return nil
}
```

### Dynamic Stage Generation

Stages can be created dynamically during workflow execution:

```go
func (a StageGeneratorAction) Execute(ctx *gostate.ActionContext) error {
    // Create a new stage dynamically
    newStage := gostate.NewStage(
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
if !ctx.Store.HasTag("important-resource", "protected") {
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
complexActions := ctx.FilterActions(func(a gostate.Action) bool {
    return strings.Contains(a.Description(), "complex")
})
```

## Use Cases

GoState is well-suited for various workflow scenarios:

- **ETL Processes** - Define data extraction, transformation and loading pipelines
- **Deployment Pipelines** - Create sequential deployment steps with conditional execution
- **Business Workflows** - Model complex business processes with state tracking
- **Resource Provisioning** - Set up resource discovery and provisioning sequences
- **Data Processing** - Orchestrate complex data operations with state management

## Examples

The repository includes several examples demonstrating different features:

- Basic workflow creation and execution
- Dynamic stage generation
- File operations workflow
- Action wrapping patterns
- Conditional execution with enable/disable

Check the `examples/` directory for complete examples.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details. 