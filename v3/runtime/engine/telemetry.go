package engine

import (
	rt "github.com/davidroman0O/gostage/v3/runtime"
	core "github.com/davidroman0O/gostage/v3/runtime/core"
)

type pendingStage struct {
	stage     rt.Stage
	createdBy string
}

// Telemetry tracks execution state while running.
type Telemetry struct {
	pendingStatus string

	stages                 map[string]*StageStatus
	actions                map[string]*ActionStatus
	dynamicStages          []DynamicStage
	dynamicActions         []DynamicAction
	pendingStageInsertions []pendingStage
	removedStageMap        map[string]string
	removedActionMap       map[string]string
}

// NewTelemetry creates a telemetry tracker for the provided workflow.
func NewTelemetry(workflow rt.Workflow, pendingStatus ExecutionStatus) *Telemetry {
	if pendingStatus == "" {
		pendingStatus = StatusPending
	}
	t := &Telemetry{
		pendingStatus:          string(pendingStatus),
		stages:                 make(map[string]*StageStatus),
		actions:                make(map[string]*ActionStatus),
		dynamicStages:          make([]DynamicStage, 0),
		dynamicActions:         make([]DynamicAction, 0),
		pendingStageInsertions: make([]pendingStage, 0),
		removedStageMap:        make(map[string]string),
		removedActionMap:       make(map[string]string),
	}
	for _, stage := range workflow.Stages() {
		if stage == nil {
			continue
		}
		t.stages[stage.ID()] = &StageStatus{
			ID:          stage.ID(),
			Name:        stage.Name(),
			Description: stage.Description(),
			Tags:        copyStrings(stage.Tags()),
			Status:      pendingStatus,
		}
	}
	return t
}

// DisabledStages returns the current disabled stage map from the execution context.
func (t *Telemetry) DisabledStages(execCtx core.ExecutionContext) map[string]bool {
	if _, stageMap := execCtx.DisabledMaps(); stageMap != nil {
		return stageMap
	}
	return map[string]bool{}
}

// Stage returns the telemetry entry for a stage, creating it if necessary.
func (t *Telemetry) Stage(id, name, description string, tags []string, dynamic bool, createdBy string) *StageStatus {
	status, ok := t.stages[id]
	if !ok {
		status = &StageStatus{ID: id, Status: StatusPending}
		if t.pendingStatus != "" {
			status.Status = ExecutionStatus(t.pendingStatus)
		}
		t.stages[id] = status
	}
	if name != "" {
		status.Name = name
	}
	if description != "" {
		status.Description = description
	}
	if len(tags) > 0 {
		status.Tags = copyStrings(tags)
	}
	if dynamic {
		status.Dynamic = true
	}
	if createdBy != "" {
		status.CreatedBy = createdBy
	}
	return status
}

// Action returns the telemetry entry for an action, creating it if necessary.
func (t *Telemetry) Action(stageID, name, description string, tags []string, dynamic bool, createdBy string) *ActionStatus {
	key := ActionKey(stageID, name)
	status, ok := t.actions[key]
	if !ok {
		status = &ActionStatus{StageID: stageID, Name: name, Status: ExecutionStatus(t.pendingStatus)}
		t.actions[key] = status
	}
	if description != "" {
		status.Description = description
	}
	if len(tags) > 0 {
		status.Tags = copyStrings(tags)
	}
	if dynamic {
		status.Dynamic = true
	}
	if createdBy != "" {
		status.CreatedBy = createdBy
	}
	return status
}

// StageStatuses returns a snapshot of all stage statuses.
func (t *Telemetry) StageStatuses() []StageStatus {
	result := make([]StageStatus, 0, len(t.stages))
	for _, status := range t.stages {
		copyStatus := *status
		if len(copyStatus.Tags) > 0 {
			copyStatus.Tags = copyStrings(copyStatus.Tags)
		}
		result = append(result, copyStatus)
	}
	return result
}

// ActionStatuses returns a snapshot of all action statuses.
func (t *Telemetry) ActionStatuses() []ActionStatus {
	result := make([]ActionStatus, 0, len(t.actions))
	for _, status := range t.actions {
		copyStatus := *status
		if len(copyStatus.Tags) > 0 {
			copyStatus.Tags = copyStrings(copyStatus.Tags)
		}
		result = append(result, copyStatus)
	}
	return result
}

// DynamicStages returns registered dynamic stages.
func (t *Telemetry) DynamicStages() []DynamicStage {
	if len(t.dynamicStages) == 0 {
		return nil
	}
	stages := make([]DynamicStage, len(t.dynamicStages))
	copy(stages, t.dynamicStages)
	return stages
}

// DynamicActions returns registered dynamic actions.
func (t *Telemetry) DynamicActions() []DynamicAction {
	if len(t.dynamicActions) == 0 {
		return nil
	}
	actions := make([]DynamicAction, len(t.dynamicActions))
	copy(actions, t.dynamicActions)
	return actions
}

// RemovedStages returns a copy of removed stage mappings.
func (t *Telemetry) RemovedStages() map[string]string {
	if len(t.removedStageMap) == 0 {
		return nil
	}
	result := make(map[string]string, len(t.removedStageMap))
	for k, v := range t.removedStageMap {
		result[k] = v
	}
	return result
}

// RemovedActions returns a copy of removed action mappings.
func (t *Telemetry) RemovedActions() map[string]string {
	if len(t.removedActionMap) == 0 {
		return nil
	}
	result := make(map[string]string, len(t.removedActionMap))
	for k, v := range t.removedActionMap {
		result[k] = v
	}
	return result
}

// AddDynamicStage records a dynamic stage and queues it for insertion.
func (t *Telemetry) AddDynamicStage(stage rt.Stage, createdBy string) {
	if stage == nil {
		return
	}
	t.pendingStageInsertions = append(t.pendingStageInsertions, pendingStage{
		stage:     stage,
		createdBy: createdBy,
	})
	t.dynamicStages = append(t.dynamicStages, DynamicStage{
		Stage:     stage,
		CreatedBy: createdBy,
	})
}

// AddDynamicAction records a dynamic action.
func (t *Telemetry) AddDynamicAction(stageID string, action rt.Action, createdBy string) {
	if action == nil {
		return
	}
	t.dynamicActions = append(t.dynamicActions, DynamicAction{
		StageID:   stageID,
		Action:    action,
		CreatedBy: createdBy,
	})
}

// InsertPendingStages inserts pending dynamic stages into the provided slice.
func (t *Telemetry) InsertPendingStages(existing []rt.Stage, index int) []rt.Stage {
	if len(t.pendingStageInsertions) == 0 {
		return existing
	}
	stages := make([]rt.Stage, 0, len(existing)+len(t.pendingStageInsertions))
	stages = append(stages, existing[:index]...)
	for _, pending := range t.pendingStageInsertions {
		stages = append(stages, pending.stage)
	}
	stages = append(stages, existing[index:]...)

	for _, pending := range t.pendingStageInsertions {
		status := t.Stage(pending.stage.ID(), pending.stage.Name(), pending.stage.Description(), pending.stage.Tags(), true, pending.createdBy)
		if status.Status == "" {
			status.Status = ExecutionStatus(t.pendingStatus)
		}
	}
	t.pendingStageInsertions = t.pendingStageInsertions[:0]
	return stages
}

// HasPendingStages reports whether dynamic stages are queued for insertion.
func (t *Telemetry) HasPendingStages() bool {
	return len(t.pendingStageInsertions) > 0
}

// MarkActionRemoved records an action removal.
func (t *Telemetry) MarkActionRemoved(stageID, actionName, createdBy string) {
	t.Action(stageID, actionName, "", nil, createdBy != "", createdBy).Status = StatusRemoved
	if createdBy != "" {
		if t.removedActionMap == nil {
			t.removedActionMap = make(map[string]string)
		}
		t.removedActionMap[ActionKey(stageID, actionName)] = createdBy
	}
}

// MarkStageRemoved records a stage removal.
func (t *Telemetry) MarkStageRemoved(stageID, createdBy string) {
	t.Stage(stageID, stageID, "", nil, createdBy != "", createdBy).Status = StatusRemoved
	if createdBy != "" {
		if t.removedStageMap == nil {
			t.removedStageMap = make(map[string]string)
		}
		t.removedStageMap[stageID] = createdBy
	}
}

func copyStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	cloned := make([]string, len(values))
	copy(cloned, values)
	return cloned
}

// ActionKey builds the canonical identifier for a stage/action combination.
func ActionKey(stageID, actionName string) string {
	return stageID + "::" + actionName
}
