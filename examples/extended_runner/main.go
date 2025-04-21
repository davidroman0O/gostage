package main

import (
	"context"
	"fmt"
	"time"

	"github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/examples/common"
	"github.com/davidroman0O/gostage/store"
)

// ConfigProvider represents an interface for accessing configuration
type ConfigProvider interface {
	GetValue(key string) (string, error)
	GetValues(prefix string) (map[string]string, error)
}

// SimpleConfigProvider is a basic implementation of ConfigProvider
type SimpleConfigProvider struct {
	values map[string]string
}

// NewSimpleConfigProvider creates a new config provider with initial values
func NewSimpleConfigProvider(values map[string]string) *SimpleConfigProvider {
	return &SimpleConfigProvider{
		values: values,
	}
}

// GetValue retrieves a single configuration value
func (p *SimpleConfigProvider) GetValue(key string) (string, error) {
	if value, exists := p.values[key]; exists {
		return value, nil
	}
	return "", fmt.Errorf("configuration key not found: %s", key)
}

// GetValues retrieves all configuration values with a given prefix
func (p *SimpleConfigProvider) GetValues(prefix string) (map[string]string, error) {
	result := make(map[string]string)
	for k, v := range p.values {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			result[k] = v
		}
	}
	return result, nil
}

// DeploymentEnvironment represents an environment for deployment
type DeploymentEnvironment struct {
	Name        string            `json:"name"`
	URL         string            `json:"url"`
	Credentials map[string]string `json:"credentials"`
	IsActive    bool              `json:"isActive"`
}

// ResourceManager manages access to deployment resources
type ResourceManager struct {
	environments map[string]*DeploymentEnvironment
}

// NewResourceManager creates a new resource manager
func NewResourceManager() *ResourceManager {
	return &ResourceManager{
		environments: make(map[string]*DeploymentEnvironment),
	}
}

// AddEnvironment adds a deployment environment
func (m *ResourceManager) AddEnvironment(env *DeploymentEnvironment) {
	m.environments[env.Name] = env
}

// GetEnvironment retrieves a deployment environment by name
func (m *ResourceManager) GetEnvironment(name string) (*DeploymentEnvironment, error) {
	if env, exists := m.environments[name]; exists {
		return env, nil
	}
	return nil, fmt.Errorf("environment not found: %s", name)
}

// GetActiveEnvironments returns all active environments
func (m *ResourceManager) GetActiveEnvironments() []*DeploymentEnvironment {
	var result []*DeploymentEnvironment
	for _, env := range m.environments {
		if env.IsActive {
			result = append(result, env)
		}
	}
	return result
}

// ExtendedRunner extends the basic runner with domain-specific functionality
type ExtendedRunner struct {
	// Embed the base runner
	*gostage.Runner

	// Domain-specific components
	configProvider  ConfigProvider
	resourceManager *ResourceManager

	// Additional settings
	defaultEnvironment string
	setupTimeout       time.Duration
}

// NewExtendedRunner creates a new extended runner with custom configuration
func NewExtendedRunner(configProvider ConfigProvider, resourceManager *ResourceManager) *ExtendedRunner {
	return &ExtendedRunner{
		Runner:             gostage.NewRunner(),
		configProvider:     configProvider,
		resourceManager:    resourceManager,
		defaultEnvironment: "development",
		setupTimeout:       30 * time.Second,
	}
}

// WithDefaultEnvironment sets the default environment
func (r *ExtendedRunner) WithDefaultEnvironment(env string) *ExtendedRunner {
	r.defaultEnvironment = env
	return r
}

// WithSetupTimeout sets the timeout for workflow setup
func (r *ExtendedRunner) WithSetupTimeout(timeout time.Duration) *ExtendedRunner {
	r.setupTimeout = timeout
	return r
}

// WithMiddleware adds middleware to the runner
func (r *ExtendedRunner) WithMiddleware(middleware ...gostage.Middleware) *ExtendedRunner {
	r.Runner.Use(middleware...)
	return r
}

// prepareWorkflow initializes a workflow with necessary resources (private method)
func (r *ExtendedRunner) prepareWorkflow(wf *gostage.Workflow) error {
	// Store the resource manager in the workflow's store
	if err := wf.Store.Put("system.resourceManager", r.resourceManager); err != nil {
		return fmt.Errorf("failed to store resource manager: %w", err)
	}

	// Instead of storing the provider (which might not serialize properly),
	// store all configuration values directly in the workflow store
	if provider, ok := r.configProvider.(*SimpleConfigProvider); ok {
		// Log the available config keys
		fmt.Println("Storing config values in workflow store:")
		for k, v := range provider.values {
			configKey := "config." + k
			fmt.Printf("  - %s: %s\n", configKey, v)
			if err := wf.Store.Put(configKey, v); err != nil {
				return fmt.Errorf("failed to store config value %s: %w", k, err)
			}
		}
	} else {
		return fmt.Errorf("unsupported config provider type: %T", r.configProvider)
	}

	// Set default environment
	if err := wf.Store.Put("deployment.defaultEnvironment", r.defaultEnvironment); err != nil {
		return fmt.Errorf("failed to set default environment: %w", err)
	}

	// Load environment-specific configuration
	env, err := r.resourceManager.GetEnvironment(r.defaultEnvironment)
	if err != nil {
		return fmt.Errorf("failed to get default environment: %w", err)
	}

	if err := wf.Store.Put("deployment.environment", env); err != nil {
		return fmt.Errorf("failed to store environment: %w", err)
	}

	// Add system metadata and tags
	wf.AddTag("prepared")
	meta, err := wf.Store.GetMetadata("workflow:" + wf.ID)
	if err != nil {
		return fmt.Errorf("failed to get workflow metadata: %w", err)
	}
	meta.SetProperty("preparedAt", time.Now().Format(time.RFC3339))

	return nil
}

// Execute overrides the base Runner's Execute method to add preparation
func (r *ExtendedRunner) Execute(ctx context.Context, wf *gostage.Workflow, logger gostage.Logger) error {
	// Create a context with timeout for the setup
	setupCtx, cancel := context.WithTimeout(ctx, r.setupTimeout)
	defer cancel()

	// Prepare the workflow with the timeout context
	logger.Info("Preparing workflow: %s", wf.ID)
	select {
	case <-setupCtx.Done():
		return fmt.Errorf("workflow preparation timed out after %v", r.setupTimeout)
	default:
		if err := r.prepareWorkflow(wf); err != nil {
			return fmt.Errorf("workflow preparation failed: %w", err)
		}
	}

	// Execute the workflow using the base Runner's Execute method
	logger.Info("Executing prepared workflow: %s", wf.ID)
	return r.Runner.Execute(ctx, wf, logger)
}

// Helper functions for actions to retrieve specialized resources from the store

// GetResourceManager retrieves the resource manager from the context
func GetResourceManager(ctx *gostage.ActionContext) (*ResourceManager, error) {
	return store.Get[*ResourceManager](ctx.Store, "system.resourceManager")
}

// GetDeploymentEnvironment retrieves the current deployment environment
func GetDeploymentEnvironment(ctx *gostage.ActionContext) (*DeploymentEnvironment, error) {
	return store.Get[*DeploymentEnvironment](ctx.Store, "deployment.environment")
}

// GetConfig retrieves a configuration value directly from the store
func GetConfig(ctx *gostage.ActionContext, key string) (string, error) {
	configKey := "config." + key
	value, err := store.Get[string](ctx.Store, configKey)
	if err != nil {
		return "", fmt.Errorf("failed to get config value for %s: %w", key, err)
	}
	return value, nil
}

// DeploymentAction demonstrates using the extended runner's helpers
type DeploymentAction struct {
	gostage.BaseAction
}

// NewDeploymentAction creates a new deployment action
func NewDeploymentAction(name, description string) *DeploymentAction {
	return &DeploymentAction{
		BaseAction: gostage.NewBaseAction(name, description),
	}
}

// Execute performs the deployment
func (a *DeploymentAction) Execute(ctx *gostage.ActionContext) error {
	// Get the deployment environment
	env, err := GetDeploymentEnvironment(ctx)
	if err != nil {
		return fmt.Errorf("failed to get deployment environment: %w", err)
	}

	ctx.Logger.Info("Deploying to environment: %s (%s)", env.Name, env.URL)

	// Get a specific configuration value
	apiKey, err := GetConfig(ctx, "api.key")
	if err != nil {
		return fmt.Errorf("failed to get API key: %w", err)
	}

	// Use the API key (in a real implementation this would do something)
	ctx.Logger.Info("Using API key: %s...", apiKey[:5]+"****")

	// Get the resource manager to access other resources
	resourceManager, err := GetResourceManager(ctx)
	if err != nil {
		return fmt.Errorf("failed to get resource manager: %w", err)
	}

	// Get all active environments
	activeEnvs := resourceManager.GetActiveEnvironments()
	ctx.Logger.Info("Found %d active environments", len(activeEnvs))

	return nil
}

// CreateDeploymentWorkflow builds a workflow for deployment
func CreateDeploymentWorkflow() *gostage.Workflow {
	// Create a new workflow
	wf := gostage.NewWorkflow(
		"deployment-workflow",
		"Deployment Workflow",
		"Demonstrates using an extended runner",
	)

	// Create a deployment stage
	deployStage := gostage.NewStage(
		"deploy",
		"Deployment Stage",
		"Performs deployment to the target environment",
	)

	// Add deployment action
	deployStage.AddAction(NewDeploymentAction(
		"deploy-app",
		"Deploy Application",
	))

	// Add the stage to the workflow
	wf.AddStage(deployStage)

	return wf
}

// Main function to run the example
func main() {
	fmt.Println("--- Extended Runner Example ---")

	// Create the configuration provider
	configProvider := NewSimpleConfigProvider(map[string]string{
		"api.key":                "secret-api-key-12345",
		"db.connection":          "postgres://user:pass@localhost:5432/db",
		"deployment.region":      "us-west-2",
		"deployment.retry.count": "3",
		"deployment.retry.delay": "5s",
	})

	// Create the resource manager
	resourceManager := NewResourceManager()

	// Add some deployment environments
	resourceManager.AddEnvironment(&DeploymentEnvironment{
		Name:     "development",
		URL:      "https://dev.example.com",
		IsActive: true,
		Credentials: map[string]string{
			"username": "dev-user",
			"password": "dev-password",
		},
	})

	resourceManager.AddEnvironment(&DeploymentEnvironment{
		Name:     "staging",
		URL:      "https://staging.example.com",
		IsActive: true,
		Credentials: map[string]string{
			"username": "staging-user",
			"password": "staging-password",
		},
	})

	resourceManager.AddEnvironment(&DeploymentEnvironment{
		Name:     "production",
		URL:      "https://production.example.com",
		IsActive: false,
		Credentials: map[string]string{
			"username": "prod-user",
			"password": "prod-password",
		},
	})

	// Create the extended runner
	extendedRunner := NewExtendedRunner(configProvider, resourceManager)

	// Configure the runner with fluent API
	extendedRunner.
		WithDefaultEnvironment("staging").
		WithSetupTimeout(10 * time.Second).
		WithMiddleware(gostage.LoggingMiddleware())

	// Create the workflow
	wf := CreateDeploymentWorkflow()

	// Print workflow information
	fmt.Printf("Workflow: %s - %s\n", wf.ID, wf.Name)
	fmt.Printf("Description: %s\n", wf.Description)
	fmt.Printf("Stages: %d\n\n", len(wf.Stages))

	// Create a context and a console logger
	ctx := context.Background()
	logger := common.NewConsoleLogger(common.LogLevelInfo)

	// Execute the workflow with the extended runner
	fmt.Println("Executing workflow with extended runner...")
	if err := extendedRunner.Execute(ctx, wf, logger); err != nil {
		fmt.Printf("Error executing workflow: %v\n", err)
		return
	}

	fmt.Println("\nWorkflow completed successfully!")
}
