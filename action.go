package gostage

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/davidroman0O/gostage/store"
)

// ActionRunnerFunc is the core function type for executing an action.
type ActionRunnerFunc func(ctx *ActionContext, action Action, index int, isLast bool) error

// ActionMiddleware represents a function that wraps action execution.
// It allows performing operations before and after an action executes,
// with information about the action's position in the execution sequence.
type ActionMiddleware func(next ActionRunnerFunc) ActionRunnerFunc

// Action is a single unit of work within a stage.
// Actions are the building blocks of workflows and represent individual tasks
// that need to be executed. Actions can be organized using tags and can be
// dynamically enabled or disabled at runtime.
type Action interface {
	// Name returns the action's name
	Name() string

	// Description returns a human-readable description of the action
	Description() string

	// Tags returns the action's tags for organization and filtering
	Tags() []string

	// Execute performs the action's work.
	// The ActionContext provides access to the workflow environment,
	// including the store for state management and the logger for output.
	Execute(ctx *ActionContext) error
}

// ActionState tracks whether an action is enabled.
// This is used to represent the runtime state of actions within a workflow.
type ActionState struct {
	// Action is a reference to the action
	Action Action
	// Enabled indicates whether the action is enabled and will be executed
	Enabled bool
}

// StageState tracks whether a stage is enabled.
// This is used to represent the runtime state of stages within a workflow.
type StageState struct {
	// Stage is a reference to the stage
	Stage *Stage
	// Enabled indicates whether the stage is enabled and will be executed
	Enabled bool
}

// ActionContext provides access to the workflow environment.
// It is passed to an Action's Execute method and provides access to
// the workflow, stage, logger, and various utilities for
// dynamic action and stage management.
type ActionContext struct {
	// GoContext is the embedded Go context
	GoContext context.Context

	// References to the current execution path
	Workflow *Workflow
	Stage    *Stage
	Action   Action

	// Logger for output and debugging
	Logger Logger

	// Dynamically generated actions (will be inserted after the current action)
	dynamicActions []Action

	// Dynamically generated stages (will be inserted after the current stage)
	dynamicStages []*Stage

	// Track actions to disable
	disabledActions map[string]bool

	// Track stages to disable
	disabledStages map[string]bool

	// Information about the action's position in execution
	ActionIndex  int
	IsLastAction bool
}

// Store returns the workflow's key-value store for data access
func (ctx *ActionContext) Store() *store.KVStore {
	return ctx.Workflow.Store
}

// It's recommended that custom actions embed this struct to handle common properties.
type BaseAction struct {
	name        string
	description string
	tags        []string
}

// GetActionBaseFields uses reflection to access BaseAction fields from any Action.
// It returns the BaseAction fields if found, or nil if the action doesn't embed BaseAction.
func GetActionBaseFields(action Action) *BaseAction {
	if action == nil {
		return nil
	}

	// Use reflection to find embedded BaseAction
	val := reflect.ValueOf(action)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil
	}

	// Look for embedded BaseAction field
	typ := val.Type()
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)

		// Check if this is an embedded BaseAction or a field of type BaseAction
		if fieldType.Type == reflect.TypeOf(BaseAction{}) {
			if field.CanAddr() {
				return field.Addr().Interface().(*BaseAction)
			}
		}

		// Also check for pointer to BaseAction
		if fieldType.Type == reflect.TypeOf(&BaseAction{}) && !field.IsNil() {
			return field.Interface().(*BaseAction)
		}
	}

	return nil
}

// NewBaseAction creates a new BaseAction with a name and description.
func NewBaseAction(name, description string) BaseAction {
	return BaseAction{
		name:        name,
		description: description,
		tags:        []string{},
	}
}

// NewBaseActionWithTags creates a new BaseAction with a name, description, and tags.
func NewBaseActionWithTags(name, description string, tags []string) BaseAction {
	return BaseAction{
		name:        name,
		description: description,
		tags:        tags,
	}
}

// Name returns the action's name.
func (a *BaseAction) Name() string {
	return a.name
}

// Description returns a human-readable description of the action.
func (a *BaseAction) Description() string {
	return a.description
}

// Tags returns the action's tags for organization and filtering.
func (a *BaseAction) Tags() []string {
	return a.tags
}

// AddTag adds a new tag to the action, avoiding duplicates.
func (a *BaseAction) AddTag(tag string) {
	for _, t := range a.tags {
		if t == tag {
			return // Tag already exists
		}
	}
	a.tags = append(a.tags, tag)
}

// AddDynamicAction adds an action to be executed immediately after the current action.
func (ctx *ActionContext) AddDynamicAction(action Action) {
	ctx.dynamicActions = append(ctx.dynamicActions, action)
}

// AddDynamicStage adds a new stage to be inserted after the current stage.
// This allows for dynamic workflow modification during execution.
// The stage will be executed immediately after the current stage completes.
func (ctx *ActionContext) AddDynamicStage(stage *Stage) {
	ctx.dynamicStages = append(ctx.dynamicStages, stage)
}

// EnableAction enables an action by name.
// If there are multiple actions with the same name, all will be enabled.
func (ctx *ActionContext) EnableAction(actionName string) {
	if ctx.disabledActions == nil {
		ctx.disabledActions = make(map[string]bool)
	}
	delete(ctx.disabledActions, actionName)
}

// DisableAction disables an action by name.
// If there are multiple actions with the same name, all will be disabled.
// Disabled actions will be skipped during workflow execution.
func (ctx *ActionContext) DisableAction(actionName string) {
	if ctx.disabledActions == nil {
		ctx.disabledActions = make(map[string]bool)
	}
	ctx.disabledActions[actionName] = true
}

// IsActionEnabled checks if an action is enabled.
// Returns true if the action is enabled or not found in the disabled actions map.
func (ctx *ActionContext) IsActionEnabled(actionName string) bool {
	if ctx.disabledActions == nil {
		return true
	}
	return !ctx.disabledActions[actionName]
}

// EnableStage enables a stage by ID.
// Enabled stages will be executed during workflow execution.
func (ctx *ActionContext) EnableStage(stageID string) {
	if ctx.disabledStages == nil {
		ctx.disabledStages = make(map[string]bool)
	}
	delete(ctx.disabledStages, stageID)
}

// DisableStage disables a stage by ID.
// Disabled stages will be skipped during workflow execution.
func (ctx *ActionContext) DisableStage(stageID string) {
	if ctx.disabledStages == nil {
		ctx.disabledStages = make(map[string]bool)
	}
	ctx.disabledStages[stageID] = true
}

// IsStageEnabled checks if a stage is enabled.
// Returns true if the stage is enabled or not found in the disabled stages map.
func (ctx *ActionContext) IsStageEnabled(stageID string) bool {
	if ctx.disabledStages == nil {
		return true
	}
	return !ctx.disabledStages[stageID]
}

// ListAllStages returns a list of all stages in the workflow.
// This includes both static stages defined at workflow creation
// and any dynamic stages added during execution.
func (ctx *ActionContext) ListAllStages() []*Stage {
	return ctx.Workflow.Stages
}

// FindStage finds a stage by ID.
// Returns the stage if found, or nil if not found.
func (ctx *ActionContext) FindStage(stageID string) *Stage {
	for _, stage := range ctx.Workflow.Stages {
		if stage.ID == stageID {
			return stage
		}
	}
	return nil
}

// RemoveStage removes a stage from the workflow by ID
func (ctx *ActionContext) RemoveStage(stageID string) bool {
	// First check if the stage is in the workflow's existing stages
	for i, stage := range ctx.Workflow.Stages {
		if stage.ID == stageID {
			// Remove the stage from the workflow
			ctx.Workflow.Stages = append(ctx.Workflow.Stages[:i], ctx.Workflow.Stages[i+1:]...)
			return true
		}
	}

	// If not found in workflow stages, check dynamicStages
	for i, stage := range ctx.dynamicStages {
		if stage.ID == stageID {
			// Remove the stage from dynamic stages
			ctx.dynamicStages = append(ctx.dynamicStages[:i], ctx.dynamicStages[i+1:]...)
			return true
		}
	}

	return false
}

// ListAllStageActions returns a list of all actions in a stage
func (ctx *ActionContext) ListAllStageActions(stageID string) []Action {
	stage := ctx.FindStage(stageID)
	if stage == nil {
		return nil
	}
	return stage.Actions
}

// ListAllActions returns a list of all actions in all stages
func (ctx *ActionContext) ListAllActions() []Action {
	var allActions []Action
	for _, stage := range ctx.Workflow.Stages {
		allActions = append(allActions, stage.Actions...)
	}
	return allActions
}

// FindAction finds an action by name across all stages
// Returns the action and its stage, or nil if not found
func (ctx *ActionContext) FindAction(actionName string) (Action, *Stage) {
	for _, stage := range ctx.Workflow.Stages {
		for _, action := range stage.Actions {
			if action.Name() == actionName {
				return action, stage
			}
		}
	}
	return nil, nil
}

// FindActionInStage finds an action by name in a specific stage
func (ctx *ActionContext) FindActionInStage(stageID, actionName string) Action {
	stage := ctx.FindStage(stageID)
	if stage == nil {
		return nil
	}

	for _, action := range stage.Actions {
		if action.Name() == actionName {
			return action
		}
	}
	return nil
}

// RemoveAction removes an action from its stage by name
// If multiple actions have the same name, only the first one is removed
func (ctx *ActionContext) RemoveAction(actionName string) bool {
	for _, stage := range ctx.Workflow.Stages {
		for i, action := range stage.Actions {
			if action.Name() == actionName {
				// Remove the action from the stage
				stage.Actions = append(stage.Actions[:i], stage.Actions[i+1:]...)
				return true
			}
		}
	}
	return false
}

// RemoveActionsByTag removes all actions with the specified tag
func (ctx *ActionContext) RemoveActionsByTag(tag string) int {
	removedCount := 0
	for _, stage := range ctx.Workflow.Stages {
		// Build a new actions list excluding those with the tag
		newActions := make([]Action, 0, len(stage.Actions))
		for _, action := range stage.Actions {
			hasTag := false
			for _, actionTag := range action.Tags() {
				if actionTag == tag {
					hasTag = true
					removedCount++
					break
				}
			}
			if !hasTag {
				newActions = append(newActions, action)
			}
		}
		stage.Actions = newActions
	}
	return removedCount
}

// RemoveActionsByType removes all actions of the specified type
func (ctx *ActionContext) RemoveActionsByType(actionType interface{}) int {
	targetType := reflect.TypeOf(actionType)
	removedCount := 0

	for _, stage := range ctx.Workflow.Stages {
		// Build a new actions list excluding those of the specified type
		newActions := make([]Action, 0, len(stage.Actions))
		for _, action := range stage.Actions {
			actionValue := reflect.ValueOf(action)
			if !actionValue.Type().AssignableTo(targetType) {
				newActions = append(newActions, action)
			} else {
				removedCount++
			}
		}
		stage.Actions = newActions
	}
	return removedCount
}

// AddActionToStage adds an action to a specific stage
func (ctx *ActionContext) AddActionToStage(stageID string, action Action) error {
	stage := ctx.FindStage(stageID)
	if stage == nil {
		return fmt.Errorf("stage '%s' not found", stageID)
	}

	stage.AddAction(action)
	return nil
}

// GetStageStates returns the states (enabled/disabled) of all stages
func (ctx *ActionContext) GetStageStates() []StageState {
	states := make([]StageState, len(ctx.Workflow.Stages))

	for i, stage := range ctx.Workflow.Stages {
		states[i] = StageState{
			Stage:   stage,
			Enabled: ctx.IsStageEnabled(stage.ID),
		}
	}

	return states
}

// GetActionStates returns the states (enabled/disabled) of all actions in a stage
func (ctx *ActionContext) GetActionStates(stageID string) []ActionState {
	stage := ctx.FindStage(stageID)
	if stage == nil {
		return nil
	}

	states := make([]ActionState, len(stage.Actions))
	for i, action := range stage.Actions {
		states[i] = ActionState{
			Action:  action,
			Enabled: ctx.IsActionEnabled(action.Name()),
		}
	}

	return states
}

// FilterStages returns stages that match the filter function
func (ctx *ActionContext) FilterStages(filter func(*Stage) bool) []*Stage {
	var result []*Stage
	for _, stage := range ctx.Workflow.Stages {
		if filter(stage) {
			result = append(result, stage)
		}
	}
	return result
}

// FilterActions returns actions that match the filter function
func (ctx *ActionContext) FilterActions(filter func(Action) bool) []Action {
	var result []Action
	for _, stage := range ctx.Workflow.Stages {
		for _, action := range stage.Actions {
			if filter(action) {
				result = append(result, action)
			}
		}
	}
	return result
}

// FindActionsByTag returns all actions with a specific tag
func (ctx *ActionContext) FindActionsByTag(tag string) []Action {
	return ctx.FilterActions(func(a Action) bool {
		for _, t := range a.Tags() {
			if t == tag {
				return true
			}
		}
		return false
	})
}

// FindActionsByTags returns all actions that have all the specified tags
func (ctx *ActionContext) FindActionsByTags(tags []string) []Action {
	return ctx.FilterActions(func(a Action) bool {
		actionTags := a.Tags()
		for _, requiredTag := range tags {
			found := false
			for _, actionTag := range actionTags {
				if actionTag == requiredTag {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	})
}

// FindActionsByAnyTag returns actions that have at least one of the specified tags
func (ctx *ActionContext) FindActionsByAnyTag(tags []string) []Action {
	return ctx.FilterActions(func(a Action) bool {
		actionTags := a.Tags()
		for _, actionTag := range actionTags {
			for _, searchTag := range tags {
				if actionTag == searchTag {
					return true
				}
			}
		}
		return false
	})
}

// FindActionsByName returns actions with names that contain the search string (case-insensitive)
func (ctx *ActionContext) FindActionsByName(nameSubstring string) []Action {
	lowerNameSubstring := strings.ToLower(nameSubstring)
	return ctx.FilterActions(func(a Action) bool {
		return strings.Contains(strings.ToLower(a.Name()), lowerNameSubstring)
	})
}

// FindActionsByExactName returns actions with names that exactly match the search string
// NOTE: This remains case-sensitive for exact matching.
func (ctx *ActionContext) FindActionsByExactName(name string) []Action {
	return ctx.FilterActions(func(a Action) bool {
		return a.Name() == name
	})
}

// FindActionsByDescription returns actions with descriptions that contain the search string (case-insensitive)
func (ctx *ActionContext) FindActionsByDescription(descSubstring string) []Action {
	lowerDescSubstring := strings.ToLower(descSubstring)
	return ctx.FilterActions(func(a Action) bool {
		return strings.Contains(strings.ToLower(a.Description()), lowerDescSubstring)
	})
}

// FindActionsByType returns actions that match the specified type
// This uses type assertions to check if an action is of a specific type
func (ctx *ActionContext) FindActionsByType(actionType interface{}) []Action {
	return ctx.FilterActions(func(a Action) bool {
		// Use reflection to check if action is of the specified type
		actionType := reflect.TypeOf(actionType)
		actionValue := reflect.ValueOf(a)
		return actionValue.Type().AssignableTo(actionType)
	})
}

// FindStagesByTag returns all stages with a specific tag
func (ctx *ActionContext) FindStagesByTag(tag string) []*Stage {
	return ctx.FilterStages(func(s *Stage) bool {
		return s.HasTag(tag)
	})
}

// FindStagesByAllTags returns all stages that have all the specified tags
func (ctx *ActionContext) FindStagesByAllTags(tags []string) []*Stage {
	return ctx.FilterStages(func(s *Stage) bool {
		return s.HasAllTags(tags)
	})
}

// FindStagesByAnyTag returns all stages that have at least one of the specified tags
func (ctx *ActionContext) FindStagesByAnyTag(tags []string) []*Stage {
	return ctx.FilterStages(func(s *Stage) bool {
		return s.HasAnyTag(tags)
	})
}

// FindStagesByName returns stages with names that contain the search string (case-insensitive)
func (ctx *ActionContext) FindStagesByName(nameSubstring string) []*Stage {
	lowerNameSubstring := strings.ToLower(nameSubstring)
	return ctx.FilterStages(func(s *Stage) bool {
		return strings.Contains(strings.ToLower(s.Name), lowerNameSubstring)
	})
}

// FindStagesByExactName returns stages with names that exactly match the search string
// NOTE: This remains case-sensitive for exact matching.
func (ctx *ActionContext) FindStagesByExactName(name string) []*Stage {
	return ctx.FilterStages(func(s *Stage) bool {
		return s.Name == name
	})
}

// FindStagesByDescription returns stages with descriptions that contain the search string (case-insensitive)
func (ctx *ActionContext) FindStagesByDescription(descSubstring string) []*Stage {
	lowerDescSubstring := strings.ToLower(descSubstring)
	return ctx.FilterStages(func(s *Stage) bool {
		return strings.Contains(strings.ToLower(s.Description), lowerDescSubstring)
	})
}

// DisableActionsByTag disables all actions with a specific tag
func (ctx *ActionContext) DisableActionsByTag(tag string) int {
	if ctx.disabledActions == nil {
		ctx.disabledActions = make(map[string]bool)
	}

	disabledCount := 0
	actions := ctx.FindActionsByTag(tag)
	for _, action := range actions {
		ctx.disabledActions[action.Name()] = true
		disabledCount++
	}
	return disabledCount
}

// EnableActionsByTag enables all actions with a specific tag
func (ctx *ActionContext) EnableActionsByTag(tag string) int {
	if ctx.disabledActions == nil {
		return 0
	}

	enabledCount := 0
	actions := ctx.FindActionsByTag(tag)
	for _, action := range actions {
		if ctx.disabledActions[action.Name()] {
			delete(ctx.disabledActions, action.Name())
			enabledCount++
		}
	}
	return enabledCount
}

// DisableActionsByType disables all actions of a specific type
func (ctx *ActionContext) DisableActionsByType(actionType interface{}) int {
	if ctx.disabledActions == nil {
		ctx.disabledActions = make(map[string]bool)
	}

	disabledCount := 0
	actions := ctx.FindActionsByType(actionType)
	for _, action := range actions {
		ctx.disabledActions[action.Name()] = true
		disabledCount++
	}
	return disabledCount
}

// EnableActionsByType enables all actions of a specific type
func (ctx *ActionContext) EnableActionsByType(actionType interface{}) int {
	if ctx.disabledActions == nil {
		return 0
	}

	enabledCount := 0
	actions := ctx.FindActionsByType(actionType)
	for _, action := range actions {
		if ctx.disabledActions[action.Name()] {
			delete(ctx.disabledActions, action.Name())
			enabledCount++
		}
	}
	return enabledCount
}

// DisableStagesByTag disables all stages with a specific tag
func (ctx *ActionContext) DisableStagesByTag(tag string) int {
	if ctx.disabledStages == nil {
		ctx.disabledStages = make(map[string]bool)
	}

	disabledCount := 0
	stages := ctx.FindStagesByTag(tag)
	for _, stage := range stages {
		ctx.disabledStages[stage.ID] = true
		disabledCount++
	}
	return disabledCount
}

// EnableStagesByTag enables all stages with a specific tag
func (ctx *ActionContext) EnableStagesByTag(tag string) int {
	if ctx.disabledStages == nil {
		return 0
	}

	enabledCount := 0
	stages := ctx.FindStagesByTag(tag)
	for _, stage := range stages {
		if ctx.disabledStages[stage.ID] {
			delete(ctx.disabledStages, stage.ID)
			enabledCount++
		}
	}
	return enabledCount
}

// Send sends a message through the Runner's broker.
// This is the primary way for an action to communicate with a parent process
// or other external listeners.
func (ctx *ActionContext) Send(msgType MessageType, payload interface{}) error {
	if runner, ok := ctx.Workflow.Context["runner"].(*Runner); ok {
		return runner.Broker.Send(msgType, payload)
	}
	return fmt.Errorf("runner not found in workflow context")
}
