package gostage

import (
	"context"

	"github.com/davidroman0O/gostage/store"
)

// StageRunnerFunc is the core function type for executing a stage.
// It follows the same pattern as RunnerFunc for workflow execution.
type StageRunnerFunc func(ctx context.Context, stage *Stage, workflow *Workflow, logger Logger) error

// StageMiddleware represents a function that wraps stage execution.
// It allows performing operations before and after a stage executes.
type StageMiddleware func(next StageRunnerFunc) StageRunnerFunc

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

	// initialStore contains key-value data available at the start of stage execution
	initialStore *store.KVStore

	// middleware contains the middleware functions to apply during stage execution
	middleware []StageMiddleware
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
		initialStore: store.NewKVStore(),
		middleware:   []StageMiddleware{},
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
		initialStore: store.NewKVStore(),
		middleware:   []StageMiddleware{},
	}
}

// Use adds middleware to the stage's middleware chain
// Middleware is executed in the order it is added
func (s *Stage) Use(middleware ...StageMiddleware) {
	s.middleware = append(s.middleware, middleware...)
}

// GetMiddleware returns the stage's middleware chain
func (s *Stage) GetMiddleware() []StageMiddleware {
	return s.middleware
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

// SetInitialData adds or updates a key-value pair in the stage's initial store
func (s *Stage) SetInitialData(key string, value any) error {
	return s.initialStore.Put(key, value)
}

// GetInitialStore returns the stage's initial store
// This is used internally by the workflow runner
func (s *Stage) getInitialStore() *store.KVStore {
	return s.initialStore
}
