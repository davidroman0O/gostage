package main

import (
	"context"
	"fmt"

	"github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/examples/common"
	"github.com/davidroman0O/gostage/store"
)

// ResourceType represents different types of resources
type ResourceType string

const (
	ResourceDatabase ResourceType = "database"
	ResourceStorage  ResourceType = "storage"
	ResourceCompute  ResourceType = "compute"
	ResourceNetwork  ResourceType = "network"
)

// Resource represents a discovered resource
type Resource struct {
	ID       string       `json:"id"`
	Type     ResourceType `json:"type"`
	Name     string       `json:"name"`
	Critical bool         `json:"critical"`
}

// ResourceFinderAction discovers resources and generates stages for each type
type ResourceFinderAction struct {
	gostage.BaseAction
}

// NewResourceFinderAction creates a new resource finder action
func NewResourceFinderAction(name, description string) *ResourceFinderAction {
	return &ResourceFinderAction{
		BaseAction: gostage.NewBaseAction(name, description),
	}
}

// Execute implements resource discovery behavior
func (a *ResourceFinderAction) Execute(ctx *gostage.ActionContext) error {
	ctx.Logger.Info("Scanning for resources...")

	// In a real implementation, this would discover actual resources
	// For this example, we'll simulate finding different resource types
	resources := []Resource{
		{ID: "db1", Type: ResourceDatabase, Name: "User Database", Critical: true},
		{ID: "s3bucket", Type: ResourceStorage, Name: "Asset Storage", Critical: false},
		{ID: "vm1", Type: ResourceCompute, Name: "Application Server", Critical: true},
		{ID: "vpc1", Type: ResourceNetwork, Name: "Main VPC", Critical: false},
	}

	// Store the resources for reference
	ctx.Store().Put("discovered.resources", resources)
	ctx.Logger.Info("Discovered %d resources", len(resources))

	// For each discovered resource, create a processing stage
	for _, resource := range resources {
		// Create a new stage for this resource type
		stage := gostage.NewStage(
			fmt.Sprintf("process-%s", resource.Type),
			fmt.Sprintf("Process %s Resources", resource.Type),
			fmt.Sprintf("Process all %s resources", resource.Type),
		)

		// Add appropriate tags
		stage.AddTag(string(resource.Type))

		// Add initial data to the stage's store
		stage.SetInitialData("resource.type", resource.Type)

		// Add the appropriate action for this resource type
		stage.AddAction(&ResourceProcessorAction{
			BaseAction: gostage.NewBaseAction(
				fmt.Sprintf("process-%s", resource.Type),
				fmt.Sprintf("Process %s Resources", resource.Type),
			),
			resourceType: resource.Type,
		})

		// Add this stage to be executed after the current one
		ctx.AddDynamicStage(stage)
	}

	return nil
}

// ResourceProcessorAction processes resources of a specific type
type ResourceProcessorAction struct {
	gostage.BaseAction
	resourceType ResourceType
}

// NewResourceProcessorAction creates a new resource processor
func NewResourceProcessorAction(name, description string, resourceType ResourceType) *ResourceProcessorAction {
	return &ResourceProcessorAction{
		BaseAction:   gostage.NewBaseAction(name, description),
		resourceType: resourceType,
	}
}

// Execute processes resources of a specific type
func (a *ResourceProcessorAction) Execute(ctx *gostage.ActionContext) error {
	// Get all discovered resources
	resources, err := store.Get[[]Resource](ctx.Store(), "discovered.resources")
	if err != nil {
		return fmt.Errorf("failed to get resources: %w", err)
	}

	// Filter resources by type
	var typeResources []Resource
	for _, resource := range resources {
		if resource.Type == a.resourceType {
			typeResources = append(typeResources, resource)
		}
	}

	ctx.Logger.Info("Processing %d resources of type %s", len(typeResources), a.resourceType)

	// Process each resource (in a real implementation, this would do actual work)
	for _, resource := range typeResources {
		ctx.Logger.Info("Processing resource: %s (%s)", resource.Name, resource.ID)

		// Store processing results
		resultKey := fmt.Sprintf("processed.%s.%s", a.resourceType, resource.ID)
		ctx.Store().Put(resultKey, true)
	}

	// Generate new dynamic actions based on processed resources
	if a.resourceType == ResourceDatabase || a.resourceType == ResourceStorage {
		// For database and storage resources, add a backup action
		ctx.AddDynamicAction(NewResourceBackupAction(
			fmt.Sprintf("backup-%s", a.resourceType),
			fmt.Sprintf("Backup %s Resources", a.resourceType),
			a.resourceType,
		))
		ctx.Logger.Info("Added dynamic backup action for %s resources", a.resourceType)
	}

	return nil
}

// ResourceBackupAction is a dynamically added action for backing up resources
type ResourceBackupAction struct {
	gostage.BaseAction
	resourceType ResourceType
}

// NewResourceBackupAction creates a new resource backup action
func NewResourceBackupAction(name, description string, resourceType ResourceType) *ResourceBackupAction {
	return &ResourceBackupAction{
		BaseAction:   gostage.NewBaseActionWithTags(name, description, []string{"backup", string(resourceType)}),
		resourceType: resourceType,
	}
}

// Execute performs backup operations
func (a *ResourceBackupAction) Execute(ctx *gostage.ActionContext) error {
	ctx.Logger.Info("Backing up %s resources...", a.resourceType)

	// Get all discovered resources
	resources, err := store.Get[[]Resource](ctx.Store(), "discovered.resources")
	if err != nil {
		return fmt.Errorf("failed to get resources: %w", err)
	}

	// Filter resources by type
	var typeResources []Resource
	for _, resource := range resources {
		if resource.Type == a.resourceType {
			typeResources = append(typeResources, resource)
		}
	}

	// Simulate backup operations
	for _, resource := range typeResources {
		backupID := fmt.Sprintf("backup-%s-%s", a.resourceType, resource.ID)
		ctx.Logger.Info("Created backup for %s: %s", resource.Name, backupID)

		// Store backup metadata
		ctx.Store().Put(fmt.Sprintf("backup.%s", resource.ID), backupID)
	}

	return nil
}

// CreateDynamicStagesWorkflow builds a workflow demonstrating dynamic stage generation
func CreateDynamicStagesWorkflow() *gostage.Workflow {
	// Create a new workflow
	wf := gostage.NewWorkflow(
		"dynamic-stages-demo",
		"Dynamic Stages Demonstration",
		"Demonstrates dynamic stage generation based on discoveries",
	)

	// Create the initial discovery stage
	discoveryStage := gostage.NewStage(
		"discovery",
		"Resource Discovery",
		"Discovers resources and generates processing stages",
	)

	// Add resource finder action that will create dynamic stages
	discoveryStage.AddAction(NewResourceFinderAction(
		"find-resources",
		"Find Resources",
	))

	// Create a final reporting stage
	reportStage := gostage.NewStage(
		"report",
		"Processing Report",
		"Generates a report of all processed resources",
	)

	reportStage.AddAction(NewReportAction(
		"generate-report",
		"Generate Processing Report",
	))

	// Add stages to workflow
	wf.AddStage(discoveryStage)
	wf.AddStage(reportStage)

	return wf
}

// ReportAction generates a report on processed resources
type ReportAction struct {
	gostage.BaseAction
}

// NewReportAction creates a new report action
func NewReportAction(name, description string) *ReportAction {
	return &ReportAction{
		BaseAction: gostage.NewBaseAction(name, description),
	}
}

// Execute generates a processing report
func (a *ReportAction) Execute(ctx *gostage.ActionContext) error {
	ctx.Logger.Info("Generating resource processing report")

	// Get all discovered resources
	resources, err := store.Get[[]Resource](ctx.Store(), "discovered.resources")
	if err != nil {
		return fmt.Errorf("failed to get resources: %w", err)
	}

	// Create summary report
	summary := make(map[ResourceType]int)
	backed := make(map[ResourceType]int)

	for _, resource := range resources {
		// Count processed resources
		resultKey := fmt.Sprintf("processed.%s.%s", resource.Type, resource.ID)
		if processed, err := store.Get[bool](ctx.Store(), resultKey); err == nil && processed {
			summary[resource.Type]++
		}

		// Count backed up resources
		backupKey := fmt.Sprintf("backup.%s", resource.ID)
		if _, err := store.Get[string](ctx.Store(), backupKey); err == nil {
			backed[resource.Type]++
		}
	}

	// Log report
	ctx.Logger.Info("Processing Summary:")
	for rType, count := range summary {
		ctx.Logger.Info("- %s: %d processed, %d backed up", rType, count, backed[rType])
	}

	// Store report for future reference
	ctx.Store().Put("processing.report", map[string]interface{}{
		"summary": summary,
		"backups": backed,
	})

	return nil
}

// Main function to run the example
func main() {
	fmt.Println("--- Dynamic Stages Workflow Example ---")

	// Create the workflow
	wf := CreateDynamicStagesWorkflow()

	// Print workflow information
	fmt.Printf("Workflow: %s - %s\n", wf.ID, wf.Name)
	fmt.Printf("Description: %s\n", wf.Description)
	fmt.Printf("Initial Stages: %d\n\n", len(wf.Stages))

	fmt.Println("Executing resource discovery workflow...")

	// Create a context and a console logger
	ctx := context.Background()
	logger := common.NewConsoleLogger(common.LogLevelInfo)

	// Create a runner
	runner := gostage.NewRunner()

	// Execute the workflow
	if err := runner.Execute(ctx, wf, logger); err != nil {
		fmt.Printf("Error executing workflow: %v\n", err)
	} else {
		fmt.Println("\nWorkflow completed successfully!")
	}

	fmt.Println("\nFinal workflow structure:")
	fmt.Printf("- Workflow: %s (%s)\n", wf.Name, wf.ID)
	fmt.Printf("- Total stages: %d\n", len(wf.Stages))
}
