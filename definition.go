package gostage

import (
	"encoding/json"
	"fmt"
)

// ActionDef is a JSON-serializable action reference for workflow definitions.
type ActionDef struct {
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	Tags        []string `json:"tags,omitempty"`
}

// StageDef is a JSON-serializable sequential stage containing actions.
type StageDef struct {
	Name    string      `json:"name"`
	Actions []ActionDef `json:"actions"`
}

// SubWorkflowDef is a JSON-serializable workflow definition for child process transfer.
type SubWorkflowDef struct {
	ID           string         `json:"id"`
	Name         string         `json:"name"`
	Stages       []StageDef     `json:"stages"`
	InitialStore map[string]any `json:"initial_store,omitempty"`
}

// WorkflowToDefinition converts a compiled Workflow to a serializable SubWorkflowDef.
// Only single-task and stage steps are supported for serialization.
func WorkflowToDefinition(wf *Workflow) *SubWorkflowDef {
	def := &SubWorkflowDef{
		ID:   wf.ID,
		Name: wf.Name,
	}

	for _, s := range wf.steps {
		switch s.kind {
		case stepSingle:
			// Each single task becomes a stage with one action
			td := lookupTask(s.taskName)
			ad := ActionDef{Name: s.taskName}
			if td != nil {
				ad.Description = td.description
				ad.Tags = td.tags
			}
			def.Stages = append(def.Stages, StageDef{
				Name:    s.name,
				Actions: []ActionDef{ad},
			})

		case stepStage:
			// A stage with multiple refs
			stage := StageDef{Name: s.name}
			for _, ref := range s.refs {
				if ref.subWorkflow != nil {
					continue // sub-workflows within stages not supported in serialization
				}
				td := lookupTask(ref.taskName)
				ad := ActionDef{Name: ref.taskName}
				if td != nil {
					ad.Description = td.description
					ad.Tags = td.tags
				}
				stage.Actions = append(stage.Actions, ad)
			}
			def.Stages = append(def.Stages, stage)
		}
	}

	return def
}

// MarshalWorkflowDefinition serializes a SubWorkflowDef to JSON bytes.
func MarshalWorkflowDefinition(def *SubWorkflowDef) ([]byte, error) {
	return json.Marshal(def)
}

// UnmarshalWorkflowDefinition deserializes JSON bytes to a SubWorkflowDef.
func UnmarshalWorkflowDefinition(data []byte) (*SubWorkflowDef, error) {
	var def SubWorkflowDef
	if err := json.Unmarshal(data, &def); err != nil {
		return nil, fmt.Errorf("unmarshal workflow definition: %w", err)
	}
	return &def, nil
}

// NewWorkflowFromDef rebuilds a Workflow from a serialized definition using the ActionFactory.
// Returns an error if any action factory is not registered.
func NewWorkflowFromDef(def *SubWorkflowDef) (*Workflow, error) {
	wf := &Workflow{
		ID:    def.ID,
		Name:  def.Name,
		state: newRunState("", nil),
		steps: make([]step, 0),
	}

	// Populate initial state
	for k, v := range def.InitialStore {
		wf.state.Set(k, v)
	}

	for i, stageDef := range def.Stages {
		if len(stageDef.Actions) == 1 {
			// Single action → single step
			ad := stageDef.Actions[0]
			factory := LookupActionFactory(ad.Name)
			if factory == nil {
				return nil, fmt.Errorf("action factory %q not registered", ad.Name)
			}

			wf.steps = append(wf.steps, step{
				id:       fmt.Sprintf("%s:%d", def.ID, i),
				kind:     stepSingle,
				name:     stageDef.Name,
				taskName: ad.Name,
			})
		} else {
			// Multiple actions → stage step with refs
			var refs []StepRef
			for _, ad := range stageDef.Actions {
				factory := LookupActionFactory(ad.Name)
				if factory == nil {
					return nil, fmt.Errorf("action factory %q not registered", ad.Name)
				}
				refs = append(refs, StepRef{taskName: ad.Name})
			}

			wf.steps = append(wf.steps, step{
				id:   fmt.Sprintf("%s:%d", def.ID, i),
				kind: stepStage,
				name: stageDef.Name,
				refs: refs,
			})
		}
	}

	return wf, nil
}
