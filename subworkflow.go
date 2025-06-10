package gostage

import "fmt"

// ActionDef is a serializable representation of an Action.
// It uses a registered ID to identify the action type and can hold
// arbitrary parameters for execution.
type ActionDef struct {
	// ID is the unique identifier of the action as registered in the ActionRegistry.
	ID string `json:"id"`
	// Name overrides the default name of the registered action, if provided.
	Name string `json:"name,omitempty"`
	// Description overrides the default description of the registered action, if provided.
	Description string `json:"description,omitempty"`
	// Tags will be merged with the default tags of the registered action.
	Tags []string `json:"tags,omitempty"`
	// Params are arbitrary key-value pairs that can be passed to the action
	// via the ActionContext's store.
	Params map[string]interface{} `json:"params,omitempty"`
}

// StageDef is a serializable representation of a Stage.
type StageDef struct {
	// ID is the unique identifier for the stage.
	ID string `json:"id"`
	// Name is a human-readable name for the stage.
	Name string `json:"name,omitempty"`
	// Description provides details about the stage's purpose.
	Description string `json:"description,omitempty"`
	// Tags for organization and filtering.
	Tags []string `json:"tags,omitempty"`
	// Actions is an ordered list of action definitions for this stage.
	Actions []ActionDef `json:"actions"`
}

// SubWorkflowDef is a serializable representation of a Workflow.
// This structure is designed to be passed to a child process to define
// the work it needs to perform.
type SubWorkflowDef struct {
	// ID is the unique identifier for the workflow.
	ID string `json:"id"`
	// Name is a human-readable name for the workflow.
	Name string `json:"name,omitempty"`
	// Description provides details about the workflow's purpose.
	Description string `json:"description,omitempty"`
	// Tags for organization and filtering.
	Tags []string `json:"tags,omitempty"`
	// Stages contains all the workflow's stage definitions in execution order.
	Stages []StageDef `json:"stages"`
	// InitialStore contains key-value data that will be loaded into the
	// workflow's store before execution begins. Values must be JSON-serializable.
	InitialStore map[string]interface{} `json:"initialStore,omitempty"`
}

// NewWorkflowFromDef creates a new Workflow instance from a SubWorkflowDef.
// It uses the action registry to instantiate the correct action types.
func NewWorkflowFromDef(def *SubWorkflowDef) (*Workflow, error) {
	wf := NewWorkflowWithTags(def.ID, def.Name, def.Description, def.Tags)

	// Populate the initial store
	if def.InitialStore != nil {
		for key, value := range def.InitialStore {
			if err := wf.Store.Put(key, value); err != nil {
				// We might want to decide if this should be a fatal error
			}
		}
	}

	for _, stageDef := range def.Stages {
		stage := NewStageWithTags(stageDef.ID, stageDef.Name, stageDef.Description, stageDef.Tags)
		for _, actionDef := range stageDef.Actions {
			action, err := NewActionFromRegistry(actionDef.ID)
			if err != nil {
				return nil, err
			}

			// Override properties if specified in the definition
			if base := GetActionBaseFields(action); base != nil {
				if actionDef.Name != "" {
					base.name = actionDef.Name
				}
				if actionDef.Description != "" {
					base.description = actionDef.Description
				}
				if len(actionDef.Tags) > 0 {
					base.tags = append(base.tags, actionDef.Tags...)
				}
			}

			// It's a bit tricky to apply params directly here.
			// The action itself should be responsible for looking for its params
			// in the store during its Execute method.
			// We can prefix them to avoid collisions.
			if actionDef.Params != nil {
				for pKey, pValue := range actionDef.Params {
					storeKey := fmt.Sprintf("param:%s:%s", actionDef.ID, pKey)
					wf.Store.Put(storeKey, pValue)
				}
			}

			stage.AddAction(action)
		}
		wf.AddStage(stage)
	}

	return wf, nil
}
