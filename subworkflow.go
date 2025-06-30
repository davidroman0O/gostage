package gostage

import "fmt"

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
