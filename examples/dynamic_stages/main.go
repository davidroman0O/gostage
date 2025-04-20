package main

import (
	"context"
	"fmt"

	"github.com/davidroman0O/gostate"
	"github.com/davidroman0O/gostate/examples/common"
	"github.com/davidroman0O/gostate/store"
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
	ID   string       `json:"id"`
	Type ResourceType `json:"type"`
	Name string       `json:"name"`
}

// ResourceFinderAction discovers resources and generates stages for each type
type ResourceFinderAction struct {
	gostate.BaseAction
}

// NewResourceFinderAction creates a new resource finder action
func NewResourceFinderAction(name, description string) *ResourceFinderAction {
	return &ResourceFinderAction{
		BaseAction: gostate.NewBaseAction(name, description),
	}
}

// Execute implements resource discovery behavior
func (a *ResourceFinderAction) Execute(ctx *gostate.ActionContext) error {
	ctx.Logger.Info("Scanning for resources...")

	// In a real implementation, this would discover actual resources
	// For this example, we'll simulate finding different resource types
	resources := []Resource{
		{ID: "db1", Type: ResourceDatabase, Name: "User Database"},
		{ID: "s3bucket", Type: ResourceStorage, Name: "Asset Storage"},
		{ID: "vm1", Type: ResourceCompute, Name: "Application Server"},
		{ID: "vpc1", Type: ResourceNetwork, Name: "Main VPC"},
	}

	// Store the resources for reference
	ctx.Store.Put("discovered.resources", resources)
	ctx.Logger.Info("Discovered %d resources", len(resources))

	// Create resource-specific stages based on discovered resources
	resourceTypes := map[ResourceType]bool{}

	// Group by resource type
	for _, resource := range resources {
		resourceTypes[resource.Type] = true
	}

	// Create a stage for each resource type
	for resourceType := range resourceTypes {
		// Create a new stage for this resource type
		stageName := fmt.Sprintf("%s-resources", resourceType)
		stageID := fmt.Sprintf("process-%s", resourceType)
		stageDescription := fmt.Sprintf("Process %s resources", resourceType)

		stage := gostate.NewStageWithTags(
			stageID,
			stageName,
			stageDescription,
			[]string{string(resourceType), "dynamic"},
		)

		// Store the resource type in the stage's initial store
		stage.InitialStore.Put("resource.type", string(resourceType))

		// Add resource-specific actions to the stage
		stage.AddAction(NewResourceProcessorAction(
			fmt.Sprintf("process-%s", resourceType),
			fmt.Sprintf("Process %s Resources", resourceType),
			resourceType,
		))

		// Add the stage to be inserted after the current stage
		ctx.AddDynamicStage(stage)
		ctx.Logger.Info("Created dynamic stage for %s resources", resourceType)
	}

	return nil
}

// ResourceProcessorAction processes resources of a specific type
type ResourceProcessorAction struct {
	gostate.BaseAction
	resourceType ResourceType
}

// NewResourceProcessorAction creates a new resource processor
func NewResourceProcessorAction(name, description string, resourceType ResourceType) *ResourceProcessorAction {
	return &ResourceProcessorAction{
		BaseAction:   gostate.NewBaseAction(name, description),
		resourceType: resourceType,
	}
}

// Execute processes resources of a specific type
func (a *ResourceProcessorAction) Execute(ctx *gostate.ActionContext) error {
	// Get all discovered resources
	resources, err := store.Get[[]Resource](ctx.Store, "discovered.resources")
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
		ctx.Store.Put(resultKey, true)
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
	gostate.BaseAction
	resourceType ResourceType
}

// NewResourceBackupAction creates a new resource backup action
func NewResourceBackupAction(name, description string, resourceType ResourceType) *ResourceBackupAction {
	return &ResourceBackupAction{
		BaseAction:   gostate.NewBaseActionWithTags(name, description, []string{"backup", string(resourceType)}),
		resourceType: resourceType,
	}
}

// Execute performs backup operations
func (a *ResourceBackupAction) Execute(ctx *gostate.ActionContext) error {
	ctx.Logger.Info("Backing up %s resources...", a.resourceType)

	// Get all discovered resources
	resources, err := store.Get[[]Resource](ctx.Store, "discovered.resources")
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
		ctx.Store.Put(fmt.Sprintf("backup.%s", resource.ID), backupID)
	}

	return nil
}

// CreateDynamicStagesWorkflow builds a workflow demonstrating dynamic stage generation
func CreateDynamicStagesWorkflow() *gostate.Workflow {
	// Create a new workflow
	wf := gostate.NewWorkflow(
		"dynamic-stages-demo",
		"Dynamic Stages Demonstration",
		"Demonstrates dynamic stage generation based on discoveries",
	)

	// Create the initial discovery stage
	discoveryStage := gostate.NewStage(
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
	reportStage := gostate.NewStage(
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
	gostate.BaseAction
}

// NewReportAction creates a new report action
func NewReportAction(name, description string) *ReportAction {
	return &ReportAction{
		BaseAction: gostate.NewBaseAction(name, description),
	}
}

// Execute generates a processing report
func (a *ReportAction) Execute(ctx *gostate.ActionContext) error {
	ctx.Logger.Info("Generating resource processing report")

	// Get all discovered resources
	resources, err := store.Get[[]Resource](ctx.Store, "discovered.resources")
	if err != nil {
		return fmt.Errorf("failed to get resources: %w", err)
	}

	// Create summary report
	summary := make(map[ResourceType]int)
	backed := make(map[ResourceType]int)

	for _, resource := range resources {
		// Count processed resources
		resultKey := fmt.Sprintf("processed.%s.%s", resource.Type, resource.ID)
		if processed, err := store.Get[bool](ctx.Store, resultKey); err == nil && processed {
			summary[resource.Type]++
		}

		// Count backed up resources
		backupKey := fmt.Sprintf("backup.%s", resource.ID)
		if _, err := store.Get[string](ctx.Store, backupKey); err == nil {
			backed[resource.Type]++
		}
	}

	// Log report
	ctx.Logger.Info("Processing Summary:")
	for rType, count := range summary {
		ctx.Logger.Info("- %s: %d processed, %d backed up", rType, count, backed[rType])
	}

	// Store report for future reference
	ctx.Store.Put("processing.report", map[string]interface{}{
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

	// Execute the workflow
	fmt.Println("Executing workflow...")

	// Create a context and a console logger
	ctx := context.Background()
	logger := common.NewConsoleLogger(common.LogLevelInfo)

	if err := wf.Execute(ctx, logger); err != nil {
		fmt.Printf("Error executing workflow: %v\n", err)
		return
	}

	fmt.Println("\nWorkflow completed successfully!")
}
