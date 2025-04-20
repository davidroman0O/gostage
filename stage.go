package gostate

import (
	"context"
	"fmt"

	"github.com/davidroman0O/gostate/store"
)

// Stage is a logical phase within a workflow that contains a sequence of actions.
// Stages provide organization and grouping of related actions and can be
// dynamically enabled, disabled, or generated during workflow execution.
type Stage struct {
	// ID is the unique identifier for the stage
	ID string
	// Name is a human-readable name for the stage
	Name string
	// Description provides details about the stage's purpose
	Description string
	// Actions is an ordered list of actions to execute
	Actions []Action
	// Tags for organization and filtering
	Tags []string

	// InitialStore contains key-value data available at the start of stage execution
	InitialStore *store.KVStore
}

// StageInfo holds serializable stage information for persistence and transmission.
// This is used when storing stage data in the workflow's key-value store.
type StageInfo struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
	ActionIDs   []string `json:"actionIds"`
}

// NewStage creates a new stage with the given properties.
// The stage will have empty actions and tags collections and a new key-value store.
func NewStage(id, name, description string) *Stage {
	return &Stage{
		ID:           id,
		Name:         name,
		Description:  description,
		Actions:      []Action{},
		Tags:         []string{},
		InitialStore: store.NewKVStore(),
	}
}

// NewStageWithTags creates a new stage with the given properties and tags.
// This is useful when the stage needs to be categorized or filtered by tags.
func NewStageWithTags(id, name, description string, tags []string) *Stage {
	return &Stage{
		ID:           id,
		Name:         name,
		Description:  description,
		Actions:      []Action{},
		Tags:         tags,
		InitialStore: store.NewKVStore(),
	}
}

// toStageInfo converts a Stage to a serializable StageInfo.
// This is used for storing the stage in a persistent storage.
func (s *Stage) toStageInfo() StageInfo {
	actionIDs := make([]string, len(s.Actions))
	for i, action := range s.Actions {
		actionIDs[i] = action.Name()
	}

	return StageInfo{
		ID:          s.ID,
		Name:        s.Name,
		Description: s.Description,
		Tags:        s.Tags,
		ActionIDs:   actionIDs,
	}
}

// AddTag adds a tag to the stage if it doesn't already exist.
// Tags are useful for categorization, filtering, and conditional execution.
func (s *Stage) AddTag(tag string) {
	// Check if tag already exists
	for _, t := range s.Tags {
		if t == tag {
			return
		}
	}
	s.Tags = append(s.Tags, tag)
}

// HasTag checks if the stage has a specific tag.
// Returns true if the tag is found, false otherwise.
func (s *Stage) HasTag(tag string) bool {
	for _, t := range s.Tags {
		if t == tag {
			return true
		}
	}
	return false
}

// HasAllTags checks if the stage has all the specified tags.
// Returns true only if every tag in the tags parameter is present in the stage's tags.
func (s *Stage) HasAllTags(tags []string) bool {
	for _, requiredTag := range tags {
		found := false
		for _, stageTag := range s.Tags {
			if stageTag == requiredTag {
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

// HasAnyTag checks if the stage has any of the specified tags.
// Returns true if at least one tag from the tags parameter is present in the stage's tags.
func (s *Stage) HasAnyTag(tags []string) bool {
	for _, stageTag := range s.Tags {
		for _, searchTag := range tags {
			if stageTag == searchTag {
				return true
			}
		}
	}
	return false
}

// AddAction adds a new action to the stage.
// Actions are executed in the order they are added to the stage.
func (s *Stage) AddAction(action Action) {
	s.Actions = append(s.Actions, action)
}

// Execute runs all actions in the stage sequentially.
// If dynamic actions are generated during execution, they are inserted after
// the current action and executed in the same stage.
// If dynamic stages are generated, they are stored for execution after this stage.
func (s *Stage) Execute(ctx context.Context, workflow *Workflow, logger Logger) error {
	if len(s.Actions) == 0 {
		logger.Warn("Stage '%s' has no actions to execute", s.ID)
		return nil
	}

	// Initialize the action context with disabled maps
	actionCtx := &ActionContext{
		GoContext:       ctx,
		Workflow:        workflow,
		Stage:           s,
		Action:          nil,
		Store:           workflow.Store,
		Logger:          logger,
		dynamicActions:  []Action{},
		dynamicStages:   []*Stage{},
		disabledActions: make(map[string]bool),
		disabledStages:  make(map[string]bool),
	}

	// Check if the disabled maps exist in workflow context
	if disabled, ok := workflow.Context["disabledActions"]; ok {
		if disabledMap, ok := disabled.(map[string]bool); ok {
			actionCtx.disabledActions = disabledMap
		}
	}

	if disabled, ok := workflow.Context["disabledStages"]; ok {
		if disabledMap, ok := disabled.(map[string]bool); ok {
			actionCtx.disabledStages = disabledMap
		}
	}

	// We need to execute actions one by one, as dynamic actions can be inserted during execution
	for i := 0; i < len(s.Actions); i++ {
		action := s.Actions[i]
		actionKey := PrefixAction + s.ID + ":" + action.Name()

		// Update action status in store
		workflow.Store.SetProperty(actionKey, PropStatus, StatusRunning)

		// Skip disabled actions
		if actionCtx.disabledActions[action.Name()] {
			logger.Debug("Skipping disabled action: %s", action.Name())
			workflow.Store.SetProperty(actionKey, PropStatus, StatusSkipped)
			continue
		}

		logger.Debug("Executing action %d/%d: %s", i+1, len(s.Actions), action.Name())

		// Update the context with the current action
		actionCtx.Action = action

		// Execute the action
		if err := action.Execute(actionCtx); err != nil {
			workflow.Store.SetProperty(actionKey, PropStatus, StatusFailed)
			return fmt.Errorf("action '%s' failed: %w", action.Name(), err)
		}

		// Check if the action generated new actions to be inserted
		if len(actionCtx.dynamicActions) > 0 {
			logger.Debug("Action generated %d new actions", len(actionCtx.dynamicActions))

			// Insert the new actions after the current one
			newActions := make([]Action, 0, len(s.Actions)+len(actionCtx.dynamicActions))
			newActions = append(newActions, s.Actions[:i+1]...)

			// Store each dynamic action in the KV store
			for _, dynAction := range actionCtx.dynamicActions {
				// Create a key for the action
				dynActionKey := PrefixAction + s.ID + ":" + dynAction.Name()

				// Create metadata for the action
				meta := store.NewMetadata()
				for _, tag := range dynAction.Tags() {
					meta.AddTag(tag)
				}
				meta.AddTag(TagDynamic)
				meta.Description = dynAction.Description()
				meta.SetProperty(PropCreatedBy, "action:"+action.Name())
				meta.SetProperty(PropStatus, StatusPending)

				// Store action metadata - since we can't easily serialize the actual action,
				// we just store its metadata and track it through the in-memory struct
				workflow.Store.PutWithMetadata(dynActionKey, dynAction.Description(), meta)
			}

			newActions = append(newActions, actionCtx.dynamicActions...)
			if i+1 < len(s.Actions) {
				newActions = append(newActions, s.Actions[i+1:]...)
			}
			s.Actions = newActions

			// Clear dynamic actions for the next iteration
			actionCtx.dynamicActions = []Action{}
		}

		// Check if the action generated new stages to be inserted
		if len(actionCtx.dynamicStages) > 0 {
			logger.Debug("Action generated %d new stages", len(actionCtx.dynamicStages))

			// Store the stages to be added to the workflow after this stage completes
			workflow.Context["dynamicStages"] = actionCtx.dynamicStages

			// Clear dynamic stages for the next iteration
			actionCtx.dynamicStages = []*Stage{}
		}

		logger.Debug("Completed action %d/%d: %s", i+1, len(s.Actions), action.Name())
		workflow.Store.SetProperty(actionKey, PropStatus, StatusCompleted)
	}

	// Store the updated disabled maps back in the workflow context
	workflow.Context["disabledActions"] = actionCtx.disabledActions
	workflow.Context["disabledStages"] = actionCtx.disabledStages

	return nil
}
