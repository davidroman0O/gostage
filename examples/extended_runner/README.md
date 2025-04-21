# Extended Runner Example

This example demonstrates how to extend the basic gostage Runner by composing it into a custom struct with domain-specific functionality. This approach is useful when you need to:

1. Provide specialized initialization/setup for workflows
2. Make domain-specific resources available to workflow actions
3. Add helper functions to retrieve specialized types from the workflow store
4. Create a fluent configuration API for your runner

## Key Components

### ExtendedRunner

The `ExtendedRunner` struct embeds the base `gostage.Runner` and adds:
- Domain-specific components (ConfigProvider, ResourceManager)
- Additional settings (defaultEnvironment, setupTimeout)
- Methods for configuring the runner with a fluent API
- A private `prepareWorkflow` method for initializing the workflow with necessary resources
- An overridden `Execute` method that adds preparation before execution

```go
type ExtendedRunner struct {
    // Embed the base runner
    *gostage.Runner

    // Domain-specific components
    configProvider    ConfigProvider
    resourceManager   *ResourceManager
    
    // Additional settings
    defaultEnvironment string
    setupTimeout       time.Duration
}
```

### Resource Interfaces

- `ConfigProvider`: Provides access to configuration values
- `ResourceManager`: Manages deployment environments and other resources

### Helper Functions

The example includes helper functions that actions can use to retrieve resources from the workflow store:

```go
// GetResourceManager retrieves the resource manager from the context
func GetResourceManager(ctx *gostage.ActionContext) (*ResourceManager, error)

// GetDeploymentEnvironment retrieves the current deployment environment
func GetDeploymentEnvironment(ctx *gostage.ActionContext) (*DeploymentEnvironment, error)

// GetConfig retrieves a configuration value directly from the store
func GetConfig(ctx *gostage.ActionContext, key string) (string, error)
```

## How Configuration Is Stored

Instead of trying to store the entire configuration provider in the workflow store (which can be problematic for interfaces), this example shows a better approach:

1. During the preparation phase, we extract all configuration values and store them directly in the workflow store with a prefix:

```go
// Extract and store individual configuration values
for k, v := range provider.values {
    configKey := "config." + k
    if err := wf.Store.Put(configKey, v); err != nil {
        return err
    }
}
```

2. We then provide a helper function to retrieve these values using the same prefix:

```go
func GetConfig(ctx *gostage.ActionContext, key string) (string, error) {
    configKey := "config." + key
    value, err := store.Get[string](ctx.Store, configKey)
    if err != nil {
        return "", fmt.Errorf("failed to get config value for %s: %w", key, err)
    }
    return value, nil
}
```

This approach avoids issues with storing and retrieving interfaces in the key-value store.

## Benefits of Using an Extended Runner

1. **Simplifies Actions**: Actions can focus on business logic rather than resource setup
2. **Consistent Initialization**: All workflows run with the same base resources
3. **Type Safety**: Helper functions provide type-safe access to resources
4. **Encapsulation**: Domain-specific logic is encapsulated in the runner
5. **Fluent Configuration**: Builder pattern allows for clean configuration
6. **Seamless API**: By overriding the Execute method, users get a familiar interface

## Example Usage

```go
// Create the extended runner
extendedRunner := NewExtendedRunner(configProvider, resourceManager)
    
// Configure the runner with fluent API
extendedRunner.
    WithDefaultEnvironment("staging").
    WithSetupTimeout(10 * time.Second).
    WithMiddleware(gostage.LoggingMiddleware())

// Create the workflow
wf := CreateDeploymentWorkflow()

// Execute the workflow with the extended runner - same API as the base runner!
if err := extendedRunner.Execute(ctx, wf, logger); err != nil {
    fmt.Printf("Error executing workflow: %v\n", err)
    return
}
```

## Usage in Actions

Within actions, you can use the helper functions to access the resources:

```go
func (a *DeploymentAction) Execute(ctx *gostage.ActionContext) error {
    // Get the deployment environment
    env, err := GetDeploymentEnvironment(ctx)
    if err != nil {
        return fmt.Errorf("failed to get deployment environment: %w", err)
    }

    // Get a specific configuration value
    apiKey, err := GetConfig(ctx, "api.key")
    if err != nil {
        return fmt.Errorf("failed to get API key: %w", err)
    }

    // Use the resource manager to access other resources
    resourceManager, err := GetResourceManager(ctx)
    if err != nil {
        return fmt.Errorf("failed to get resource manager: %w", err)
    }

    // Rest of your action logic...
    return nil
}
``` 