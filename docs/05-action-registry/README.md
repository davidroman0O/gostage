# The Action Registry

The Action Registry is a global, in-memory map that is essential for one of `gostage`'s most powerful features: **spawning sub-workflows**.

When you want to run a workflow in a separate child process, you cannot send the Go `Action` objects directly to it. Instead, you send a serializable `SubWorkflowDef` which describes the workflow. This definition does not contain the actions' logic, but rather their unique **IDs**.

The child process, upon receiving this definition, needs a way to reconstruct the workflow. It does this by looking up each action ID in its own Action Registry to find the corresponding function that can create an instance of that action.

## Why is it Necessary?

-   **Decoupling**: It decouples the definition of a workflow from its implementation. The parent process only needs to know *what* actions to run (their IDs), not *how* they are implemented.
-   **Serialization**: Go functions and channels cannot be easily serialized to JSON or another format to be passed between processes. The registry pattern is a standard way to solve this problem by passing identifiers instead.
-   **Security and Stability**: It ensures that the child process only executes actions that have been explicitly registered and are known to its codebase.

## How to Use It

Using the registry involves two main functions:

1.  `gostage.RegisterAction(id string, factory func() gostage.Action)`
2.  `gostage.NewActionFromRegistry(id string) (gostage.Action, error)` (used internally by the framework)

### Step 1: Define and Register Your Actions

You must register any action that you intend to use in a spawned sub-workflow. This should be done at application startup, typically in an `init()` function or early in `main()`.

**Crucially, both the parent and child processes must execute this registration code so they share the same registry.**

```go
// actions/file_actions.go

package actions

import "github.com/davidroman0O/gostage"

const CopyActionID = "actions.copy"

type CopyAction struct {
    gostage.BaseAction
}

func (a *CopyAction) Execute(ctx *gostage.ActionContext) error {
    // ... logic to copy a file ...
    return nil
}

// The registration is done in an init() function in the same package.
func init() {
    gostage.RegisterAction(CopyActionID, func() gostage.Action {
        return &CopyAction{
            BaseAction: gostage.NewBaseAction(CopyActionID, "Copies a file."),
        }
    })
}
```

### Step 2: Define the Sub-Workflow

When you define your `SubWorkflowDef`, you refer to the action by the ID you registered, not by creating an instance of it.

```go
// In the parent process logic:

// Note that we are using the action's ID string, not an instance of the action struct.
subWorkflowDef := gostage.SubWorkflowDef{
    ID: "my-sub-workflow",
    Stages: []gostage.StageDef{{
        ID: "file-operations",
        Actions: []gostage.ActionDef{
            {ID: "actions.copy"}, // Use the registered ID here
        },
    }},
}

// Now, you can spawn a child process with this definition.
// The runner automatically uses gRPC transport with no configuration needed!
runner := gostage.NewRunner() // gRPC is automatic
err := runner.Spawn(ctx, subWorkflowDef)
```

The child process will receive this definition, see the ID `"actions.copy"`, look it up in its registry, and create a new `CopyAction` instance to execute.

## Child Process Implementation

In your child process, the new seamless API makes setup trivial:

```go
func childMain() {
    // Register actions (same as parent)
    registerActions()
    
    // ✨ NEW SEAMLESS API - automatic gRPC setup and logger creation
    childRunner, logger, err := gostage.NewChildRunner()
    if err != nil {
        log.Fatal(err)
    }
    
    // ✨ Direct method call - no GetTransport() needed!
    workflowDef, err := childRunner.RequestWorkflowDefinitionFromParent(context.Background())
    if err != nil {
        log.Fatal(err)
    }
    
    // Create and execute workflow with the returned logger
    workflow, err := gostage.NewWorkflowFromDef(workflowDef)
    if err != nil {
        log.Fatal(err)
    }
    
    err = childRunner.Execute(context.Background(), workflow, logger)
    if err != nil {
        log.Fatal(err)
    }
    
    // Send final store and clean up
    if workflow.Store != nil {
        childRunner.Broker.Send(gostage.MessageTypeFinalStore, workflow.Store.ExportAll())
    }
    childRunner.Close()
}
```

## Example

For a complete, runnable example of using the Action Registry for a spawned workflow, see the code in the `examples/01-custom-action-registry` subdirectory.

---

Next, we'll look at the complete process of [**Spawning Sub-Workflows**](../06-spawning-subworkflows/README.md). 