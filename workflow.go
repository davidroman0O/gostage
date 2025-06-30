package gostage

import (
	"fmt"
	"time"

	"github.com/davidroman0O/gostage/store"
)

// Workflow is a sequence of stages forming a complete process.
// It provides the top-level coordination for executing a series of stages
// and maintaining their shared state and context.
// Workflows can be dynamically modified during execution, allowing for
// flexible and adaptable processes.
type Workflow struct {
	// ID is the unique identifier for the workflow
	ID string
	// Name is a human-readable name for the workflow
	Name string
	// Description provides details about the workflow's purpose
	Description string
	// Tags for organization and filtering
	Tags []string

	// Store is the central key-value store for workflow data
	// It stores workflow metadata, stage information, and execution data
	Store *store.KVStore

	// Stages contains all the workflow's stages in execution order
	// This provides direct access during execution
	Stages []*Stage

	// Context stores arbitrary data for use during workflow execution
	// Implementation-specific tools and state can be stored here
	Context map[string]interface{}

	// middleware contains workflow-level middleware that wraps stage execution
	middleware []WorkflowMiddleware
}

// NewWorkflow creates a new workflow with the given properties.
// It initializes empty collections for stages, tags, and context,
// and creates a new key-value store.
func NewWorkflow(id, name, description string) *Workflow {
	w := &Workflow{
		ID:          id,
		Name:        name,
		Description: description,
		Tags:        []string{},
		Store:       store.NewKVStore(),
		Stages:      []*Stage{},
		Context:     make(map[string]interface{}),
		middleware:  []WorkflowMiddleware{},
	}

	// Store workflow info in the KV store with metadata
	w.saveToStore()

	return w
}

// Use adds middleware to the workflow's middleware chain
// This middleware will be applied to each stage execution
func (w *Workflow) Use(middleware ...WorkflowMiddleware) {
	w.middleware = append(w.middleware, middleware...)
}

// GetMiddleware returns the workflow's middleware chain
func (w *Workflow) GetMiddleware() []WorkflowMiddleware {
	return w.middleware
}

// saveToStore saves or updates the workflow metadata in the store.
// This ensures that workflow information is persistently stored
// and can be retrieved later.
func (w *Workflow) saveToStore() {
	info := WorkflowInfo{
		ID:          w.ID,
		Name:        w.Name,
		Description: w.Description,
		Tags:        w.Tags,
		StageIDs:    w.getStageIDs(),
		CreatedAt:   time.Now().Format(time.RFC3339),
		UpdatedAt:   time.Now().Format(time.RFC3339),
	}

	meta := store.NewMetadata()
	meta.Tags = append(meta.Tags, w.Tags...)
	meta.Tags = append(meta.Tags, TagSystem)
	meta.Description = w.Description

	key := PrefixWorkflow + w.ID
	w.Store.PutWithMetadata(key, info, meta)
}

// getStageIDs returns the IDs of all stages in the workflow.
// This is used when saving workflow metadata to the store.
func (w *Workflow) getStageIDs() []string {
	ids := make([]string, len(w.Stages))
	for i, stage := range w.Stages {
		ids[i] = stage.ID
	}
	return ids
}

// NewWorkflowWithTags creates a new workflow with the given properties and tags.
// This is useful when the workflow needs to be categorized or filtered by tags.
func NewWorkflowWithTags(id, name, description string, tags []string) *Workflow {
	w := NewWorkflow(id, name, description)
	w.Tags = tags
	w.saveToStore()
	return w
}

// AddTag adds a tag to the workflow if it doesn't already exist.
// Tags are useful for categorization, filtering, and conditional execution.
func (w *Workflow) AddTag(tag string) {
	// Check if tag already exists
	for _, t := range w.Tags {
		if t == tag {
			return
		}
	}
	w.Tags = append(w.Tags, tag)
	w.saveToStore()
}

// HasTag checks if the workflow has a specific tag.
// Returns true if the tag is found, false otherwise.
func (w *Workflow) HasTag(tag string) bool {
	for _, t := range w.Tags {
		if t == tag {
			return true
		}
	}
	return false
}

// HasAllTags checks if the workflow has all the specified tags.
// Returns true only if every tag in the tags parameter is present in the workflow's tags.
func (w *Workflow) HasAllTags(tags []string) bool {
	for _, requiredTag := range tags {
		found := false
		for _, workflowTag := range w.Tags {
			if workflowTag == requiredTag {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// HasAnyTag checks if the workflow has any of the specified tags.
// Returns true if at least one tag from the tags parameter is present in the workflow's tags.
func (w *Workflow) HasAnyTag(tags []string) bool {
	for _, workflowTag := range w.Tags {
		for _, searchTag := range tags {
			if workflowTag == searchTag {
				return true
			}
		}
	}
	return false
}

// AddStage adds a new stage to the workflow and stores it in the KV store.
// Stages are executed in the order they are added to the workflow.
func (w *Workflow) AddStage(stage *Stage) {
	// Add to traditional Stages slice
	w.Stages = append(w.Stages, stage)

	// Store the stage in the KV store
	stageKey := PrefixStage + stage.ID
	stageInfo := stage.toStageInfo()

	meta := store.NewMetadata()
	meta.Tags = append(meta.Tags, stage.Tags...)
	meta.Description = stage.Description
	meta.SetProperty(PropOrder, len(w.Stages)-1)
	meta.SetProperty(PropStatus, StatusPending)
	meta.SetProperty(PropCreatedBy, "workflow:"+w.ID)

	w.Store.PutWithMetadata(stageKey, stageInfo, meta)

	// Update workflow info in the store
	w.saveToStore()
}

// GetStage retrieves a stage by ID from the KV store
func (w *Workflow) GetStage(stageID string) (*Stage, error) {
	// First try to find in the Stages slice for efficiency
	for _, stage := range w.Stages {
		if stage.ID == stageID {
			return stage, nil
		}
	}

	// If not found, try to get from the store
	stageKey := PrefixStage + stageID
	stageInfo, err := store.Get[StageInfo](w.Store, stageKey)
	if err != nil {
		return nil, fmt.Errorf("stage not found: %w", err)
	}

	// Convert StageInfo back to Stage
	stage := &Stage{
		ID:           stageInfo.ID,
		Name:         stageInfo.Name,
		Description:  stageInfo.Description,
		Tags:         stageInfo.Tags,
		Actions:      []Action{},
		initialStore: store.NewKVStore(),
	}

	// Load actions for this stage
	for _, actionID := range stageInfo.ActionIDs {
		action, err := w.GetAction(stageID, actionID)
		if err != nil {
			continue // Skip actions that can't be loaded
		}
		stage.Actions = append(stage.Actions, action)
	}

	return stage, nil
}

// GetAction retrieves an action from the KV store
func (w *Workflow) GetAction(stageID, actionID string) (Action, error) {
	actionKey := PrefixAction + stageID + ":" + actionID

	// Attempt to get action information from the store
	// Note: Since Action is an interface, we need a more complex deserialization approach
	// which would depend on how actions are serialized and their concrete types
	// This is a simplified placeholder implementation

	// Check if action key exists in the store
	_, err := w.Store.GetMetadata(actionKey)
	if err != nil {
		return nil, fmt.Errorf("action not found: %w", err)
	}

	// In a real implementation, we would deserialize based on the action type
	// For now, we'll just search the in-memory structure
	stage, err := w.GetStage(stageID)
	if err != nil {
		return nil, err
	}

	for _, action := range stage.Actions {
		if action.Name() == actionID {
			return action, nil
		}
	}

	return nil, fmt.Errorf("action %s not found in stage %s", actionID, stageID)
}

// GetContext returns a value from the workflow context
func (w *Workflow) GetContext(key string) (interface{}, bool) {
	val, exists := w.Context[key]
	return val, exists
}

// SetContext stores a value in the workflow context
func (w *Workflow) SetContext(key string, value interface{}) {
	w.Context[key] = value
}

// EnableAllStages enables all stages in the workflow
func (w *Workflow) EnableAllStages() {
	w.Context["disabledStages"] = make(map[string]bool)

	// Update tags in the store
	for _, stage := range w.Stages {
		stageKey := PrefixStage + stage.ID
		w.Store.RemoveTag(stageKey, TagDisabled)
	}
}

// DisableStage disables a stage by ID
func (w *Workflow) DisableStage(stageID string) {
	disabledStages, ok := w.Context["disabledStages"].(map[string]bool)
	if !ok {
		disabledStages = make(map[string]bool)
		w.Context["disabledStages"] = disabledStages
	}
	disabledStages[stageID] = true

	// Add disabled tag in the store
	stageKey := PrefixStage + stageID
	w.Store.AddTag(stageKey, TagDisabled)
}

// EnableStage enables a stage by ID
func (w *Workflow) EnableStage(stageID string) {
	disabledStages, ok := w.Context["disabledStages"].(map[string]bool)
	if !ok {
		return
	}
	delete(disabledStages, stageID)

	// Remove disabled tag from the store
	stageKey := PrefixStage + stageID
	w.Store.RemoveTag(stageKey, TagDisabled)
}

// IsStageEnabled checks if a stage is enabled
func (w *Workflow) IsStageEnabled(stageID string) bool {
	disabledStages, ok := w.Context["disabledStages"].(map[string]bool)
	if !ok {
		return true
	}
	return !disabledStages[stageID]
}

// ListStagesByTag returns all stages with a specific tag
func (w *Workflow) ListStagesByTag(tag string) []*Stage {
	var result []*Stage

	// Use the store to find stages with this tag
	keys := w.Store.FindKeysByTag(tag)
	for _, key := range keys {
		// Only process keys with the stage prefix
		if len(key) <= len(PrefixStage) || key[:len(PrefixStage)] != PrefixStage {
			continue
		}

		stageID := key[len(PrefixStage):]
		stage, err := w.GetStage(stageID)
		if err == nil {
			result = append(result, stage)
		}
	}

	return result
}

// ListStagesByStatus returns all stages with a specific status
func (w *Workflow) ListStagesByStatus(status string) []*Stage {
	var result []*Stage

	// Use the store to find stages with this status property
	keys := w.Store.FindKeysByProperty(PropStatus, status)
	for _, key := range keys {
		// Only process keys with the stage prefix
		if len(key) <= len(PrefixStage) || key[:len(PrefixStage)] != PrefixStage {
			continue
		}

		stageID := key[len(PrefixStage):]
		stage, err := w.GetStage(stageID)
		if err == nil {
			result = append(result, stage)
		}
	}

	return result
}
