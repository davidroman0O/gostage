package state

import (
	"context"
	"errors"
	"maps"
	"sort"
	"time"

	"github.com/davidroman0O/gostage/v3/internal/locks"
)

// CaptureObserver records manager events for use in tests.
type CaptureObserver struct {
	mu locks.Mutex

	workflows map[WorkflowID]WorkflowRecord
	stages    map[WorkflowID]map[string]StageRecord
	actions   map[WorkflowID]map[string]ActionHistoryRecord
	progress  map[WorkflowID]map[string]progressEntry
	summaries map[WorkflowID]ResultSummary

	workflowStatuses []WorkflowStatusEvent
	stageStatuses    []StageStatusEvent
	actionStatuses   []ActionStatusEvent
	stageRemovals    []StageRemovalEvent
	actionRemovals   []ActionRemovalEvent
	actionEvents     []ActionEventRecord
}

type progressEntry struct {
	progress int
	message  string
}

// WorkflowStatusEvent captures workflow status transitions.
type WorkflowStatusEvent struct {
	WorkflowID string
	Status     WorkflowState
}

// StageStatusEvent captures stage status transitions.
type StageStatusEvent struct {
	WorkflowID string
	StageID    string
	Status     WorkflowState
}

// ActionStatusEvent captures action status transitions.
type ActionStatusEvent struct {
	WorkflowID string
	StageID    string
	ActionID   string
	Status     WorkflowState
}

// StageRemovalEvent captures stage removal metadata.
type StageRemovalEvent struct {
	WorkflowID string
	StageID    string
	RemovedBy  string
}

// ActionRemovalEvent captures action removal metadata.
type ActionRemovalEvent struct {
	WorkflowID string
	StageID    string
	ActionID   string
	RemovedBy  string
}

// ActionEventRecord captures custom action-level events.
type ActionEventRecord struct {
	WorkflowID string
	StageID    string
	ActionID   string
	Kind       string
	Message    string
	Metadata   map[string]any
}

// CaptureSnapshot is a consistent view of the observer state.
type CaptureSnapshot struct {
	Workflows map[WorkflowID]WorkflowRecord
	Stages    map[WorkflowID]map[string]StageRecord
	Actions   map[WorkflowID]map[string]ActionHistoryRecord
	Progress  map[WorkflowID]map[string]progressEntry
	Summaries map[WorkflowID]ResultSummary

	WorkflowStatuses []WorkflowStatusEvent
	StageStatuses    []StageStatusEvent
	ActionStatuses   []ActionStatusEvent
	StageRemovals    []StageRemovalEvent
	ActionRemovals   []ActionRemovalEvent
	ActionEvents     []ActionEventRecord
}

// NewCaptureObserver constructs an empty CaptureObserver.
func NewCaptureObserver() *CaptureObserver {
	return &CaptureObserver{
		workflows: make(map[WorkflowID]WorkflowRecord),
		stages:    make(map[WorkflowID]map[string]StageRecord),
		actions:   make(map[WorkflowID]map[string]ActionHistoryRecord),
		progress:  make(map[WorkflowID]map[string]progressEntry),
		summaries: make(map[WorkflowID]ResultSummary),
	}
}

func (c *CaptureObserver) WorkflowRegistered(_ context.Context, wf WorkflowRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workflows[wf.ID] = cloneWorkflowRecord(wf)
}

func (c *CaptureObserver) WorkflowStatus(_ context.Context, workflowID string, status WorkflowState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workflowStatuses = append(c.workflowStatuses, WorkflowStatusEvent{WorkflowID: workflowID, Status: status})
	if wf, ok := c.workflows[WorkflowID(workflowID)]; ok {
		wf.State = status
		c.workflows[WorkflowID(workflowID)] = wf
	}
}

func (c *CaptureObserver) StageRegistered(_ context.Context, workflowID string, stage StageRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stages[WorkflowID(workflowID)] == nil {
		c.stages[WorkflowID(workflowID)] = make(map[string]StageRecord)
	}
	c.stages[WorkflowID(workflowID)][stage.ID] = cloneStageRecord(stage)
	if wf, ok := c.workflows[WorkflowID(workflowID)]; ok {
		if wf.Stages == nil {
			wf.Stages = make(map[string]*StageRecord)
		}
		clone := cloneStageRecord(stage)
		wf.Stages[stage.ID] = &clone
		c.workflows[WorkflowID(workflowID)] = wf
	}
}

func (c *CaptureObserver) StageStatus(_ context.Context, workflowID, stageID string, status WorkflowState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stageStatuses = append(c.stageStatuses, StageStatusEvent{WorkflowID: workflowID, StageID: stageID, Status: status})
	if stages := c.stages[WorkflowID(workflowID)]; stages != nil {
		if stage, ok := stages[stageID]; ok {
			stage.Status = status
			stages[stageID] = stage
		}
	}
	if wf, ok := c.workflows[WorkflowID(workflowID)]; ok {
		if wf.Stages == nil {
			wf.Stages = make(map[string]*StageRecord)
		}
		if stage := wf.Stages[stageID]; stage != nil {
			stage.Status = status
		} else {
			copy := StageRecord{ID: stageID, Status: status}
			wf.Stages[stageID] = &copy
		}
		c.workflows[WorkflowID(workflowID)] = wf
	}
}

func (c *CaptureObserver) ActionRegistered(_ context.Context, workflowID, stageID string, action ActionRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.actions[WorkflowID(workflowID)] == nil {
		c.actions[WorkflowID(workflowID)] = make(map[string]ActionHistoryRecord)
	}
	key := actionKey(stageID, action.Name)
	c.actions[WorkflowID(workflowID)][key] = ActionHistoryRecord{
		ActionID:    action.Name,
		StageID:     stageID,
		Ref:         action.Ref,
		Description: action.Description,
		Tags:        append([]string(nil), action.Tags...),
		Dynamic:     action.Dynamic,
		CreatedBy:   action.CreatedBy,
		State:       action.Status,
	}
	if wf, ok := c.workflows[WorkflowID(workflowID)]; ok {
		stage := wf.Stages[stageID]
		if stage == nil {
			stageClone := cloneStageRecord(StageRecord{ID: stageID})
			stage = &stageClone
			if wf.Stages == nil {
				wf.Stages = make(map[string]*StageRecord)
			}
			wf.Stages[stageID] = stage
		}
		if stage.Actions == nil {
			stage.Actions = make(map[string]*ActionRecord)
		}
		actionClone := cloneActionRecord(action)
		stage.Actions[action.Name] = &actionClone
		c.workflows[WorkflowID(workflowID)] = wf
	}
}

func (c *CaptureObserver) ActionStatus(_ context.Context, workflowID, stageID, actionID string, status WorkflowState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.actionStatuses = append(c.actionStatuses, ActionStatusEvent{
		WorkflowID: workflowID,
		StageID:    stageID,
		ActionID:   actionID,
		Status:     status,
	})
	if actions := c.actions[WorkflowID(workflowID)]; actions != nil {
		key := actionKey(stageID, actionID)
		record := actions[key]
		record.State = status
		actions[key] = record
	}
	if wf, ok := c.workflows[WorkflowID(workflowID)]; ok {
		stage := wf.Stages[stageID]
		if stage == nil {
			stageClone := cloneStageRecord(StageRecord{ID: stageID})
			stage = &stageClone
			if wf.Stages == nil {
				wf.Stages = make(map[string]*StageRecord)
			}
			wf.Stages[stageID] = stage
		}
		if stage.Actions == nil {
			stage.Actions = make(map[string]*ActionRecord)
		}
		if action := stage.Actions[actionID]; action != nil {
			action.Status = status
		} else {
			stage.Actions[actionID] = &ActionRecord{Name: actionID, Status: status}
		}
		c.workflows[WorkflowID(workflowID)] = wf
	}
}

func (c *CaptureObserver) ActionProgress(_ context.Context, workflowID, stageID, actionID string, progress int, message string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.progress[WorkflowID(workflowID)] == nil {
		c.progress[WorkflowID(workflowID)] = make(map[string]progressEntry)
	}
	key := actionKey(stageID, actionID)
	c.progress[WorkflowID(workflowID)][key] = progressEntry{progress: progress, message: message}
}

func (c *CaptureObserver) ActionEvent(_ context.Context, workflowID, stageID, actionID, kind, message string, metadata map[string]any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var meta map[string]any
	if len(metadata) > 0 {
		meta = make(map[string]any, len(metadata))
		for k, v := range metadata {
			meta[k] = v
		}
	}
	c.actionEvents = append(c.actionEvents, ActionEventRecord{
		WorkflowID: workflowID,
		StageID:    stageID,
		ActionID:   actionID,
		Kind:       kind,
		Message:    message,
		Metadata:   meta,
	})
}

func (c *CaptureObserver) ActionRemoved(_ context.Context, workflowID, stageID, actionID, createdBy string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.actionRemovals = append(c.actionRemovals, ActionRemovalEvent{
		WorkflowID: workflowID,
		StageID:    stageID,
		ActionID:   actionID,
		RemovedBy:  createdBy,
	})
	if actions := c.actions[WorkflowID(workflowID)]; actions != nil {
		key := actionKey(stageID, actionID)
		record := actions[key]
		record.State = WorkflowRemoved
		actions[key] = record
	}
}

func (c *CaptureObserver) StageRemoved(_ context.Context, workflowID, stageID, createdBy string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stageRemovals = append(c.stageRemovals, StageRemovalEvent{
		WorkflowID: workflowID,
		StageID:    stageID,
		RemovedBy:  createdBy,
	})
	if stages := c.stages[WorkflowID(workflowID)]; stages != nil {
		stage := stages[stageID]
		stage.Status = WorkflowRemoved
		stage.CreatedBy = createdBy
		stages[stageID] = stage
	}
}

func (c *CaptureObserver) ExecutionSummary(_ context.Context, workflowID string, summary ResultSummary) {
	c.mu.Lock()
	defer c.mu.Unlock()
	clone := summary
	clone.Output = maps.Clone(summary.Output)
	clone.DisabledStages = maps.Clone(summary.DisabledStages)
	clone.DisabledActions = maps.Clone(summary.DisabledActions)
	clone.RemovedStages = maps.Clone(summary.RemovedStages)
	clone.RemovedActions = maps.Clone(summary.RemovedActions)
	c.summaries[WorkflowID(workflowID)] = clone
}

// Snapshot returns a deep copy of the captured data.
func (c *CaptureObserver) Snapshot() CaptureSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := CaptureSnapshot{
		Workflows:        make(map[WorkflowID]WorkflowRecord, len(c.workflows)),
		Stages:           make(map[WorkflowID]map[string]StageRecord, len(c.stages)),
		Actions:          make(map[WorkflowID]map[string]ActionHistoryRecord, len(c.actions)),
		Progress:         make(map[WorkflowID]map[string]progressEntry, len(c.progress)),
		Summaries:        make(map[WorkflowID]ResultSummary, len(c.summaries)),
		WorkflowStatuses: append([]WorkflowStatusEvent(nil), c.workflowStatuses...),
		StageStatuses:    append([]StageStatusEvent(nil), c.stageStatuses...),
		ActionStatuses:   append([]ActionStatusEvent(nil), c.actionStatuses...),
		StageRemovals:    append([]StageRemovalEvent(nil), c.stageRemovals...),
		ActionRemovals:   append([]ActionRemovalEvent(nil), c.actionRemovals...),
		ActionEvents:     make([]ActionEventRecord, len(c.actionEvents)),
	}

	for i, evt := range c.actionEvents {
		clone := ActionEventRecord{
			WorkflowID: evt.WorkflowID,
			StageID:    evt.StageID,
			ActionID:   evt.ActionID,
			Kind:       string(evt.Kind),
			Message:    evt.Message,
		}
		if len(evt.Metadata) > 0 {
			clone.Metadata = make(map[string]any, len(evt.Metadata))
			for k, v := range evt.Metadata {
				clone.Metadata[k] = v
			}
		}
		out.ActionEvents[i] = clone
	}

	for id, wf := range c.workflows {
		out.Workflows[id] = cloneWorkflowRecord(wf)
	}
	for id, stages := range c.stages {
		dup := make(map[string]StageRecord, len(stages))
		for stageID, stage := range stages {
			dup[stageID] = cloneStageRecord(stage)
		}
		out.Stages[id] = dup
	}
	for id, actions := range c.actions {
		dup := make(map[string]ActionHistoryRecord, len(actions))
		for key, record := range actions {
			clone := record
			clone.Tags = append([]string(nil), record.Tags...)
			dup[key] = clone
		}
		out.Actions[id] = dup
	}
	for id, prog := range c.progress {
		dup := make(map[string]progressEntry, len(prog))
		for key, entry := range prog {
			dup[key] = entry
		}
		out.Progress[id] = dup
	}
	for id, summary := range c.summaries {
		clone := summary
		clone.Output = maps.Clone(summary.Output)
		clone.DisabledStages = maps.Clone(summary.DisabledStages)
		clone.DisabledActions = maps.Clone(summary.DisabledActions)
		clone.RemovedStages = maps.Clone(summary.RemovedStages)
		clone.RemovedActions = maps.Clone(summary.RemovedActions)
		out.Summaries[id] = clone
	}
	return out
}

func actionKey(stageID, actionID string) string {
	return stageID + "::" + actionID
}

// Reader exposes a StateReader backed by the captured data.
func (c *CaptureObserver) Reader() StateReader {
	return &captureStateReader{observer: c}
}

type captureStateReader struct {
	observer *CaptureObserver
}

func (r *captureStateReader) WorkflowSummary(ctx context.Context, id WorkflowID) (WorkflowSummary, error) {
	_ = ctx
	snap := r.observer.Snapshot()
	wf, ok := snap.Workflows[id]
	if !ok {
		return WorkflowSummary{}, errors.New("workflow not found")
	}
	summary := WorkflowSummary{WorkflowRecord: cloneWorkflowRecord(wf)}
	if res, ok := snap.Summaries[id]; ok {
		summary.Success = res.Success
		summary.Error = res.Error
		summary.Duration = res.Duration
		summary.WorkflowRecord.Success = res.Success
		summary.WorkflowRecord.Error = res.Error
		summary.WorkflowRecord.Duration = res.Duration
		if res.Reason != "" {
			summary.WorkflowRecord.TerminationReason = res.Reason
		}
		if !res.CompletedAt.IsZero() {
			completed := res.CompletedAt
			summary.CompletedAt = &completed
			summary.WorkflowRecord.CompletedAt = &completed
		}
	}
	return summary, nil
}

func (r *captureStateReader) ListWorkflows(ctx context.Context, filter StateFilter) ([]WorkflowSummary, error) {
	_ = ctx
	snap := r.observer.Snapshot()
	ids := make([]WorkflowID, 0, len(snap.Workflows))
	for id := range snap.Workflows {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		left := snap.Workflows[ids[i]].CreatedAt
		right := snap.Workflows[ids[j]].CreatedAt
		if left.Equal(right) {
			return ids[i] < ids[j]
		}
		return left.Before(right)
	})

	matchesState := func(target WorkflowState) bool {
		if len(filter.States) == 0 {
			return true
		}
		for _, st := range filter.States {
			if st == target {
				return true
			}
		}
		return false
	}

	matchesTags := func(have []string) bool {
		if len(filter.Tags) == 0 {
			return true
		}
		set := make(map[string]struct{}, len(have))
		for _, tag := range have {
			set[tag] = struct{}{}
		}
		for _, tag := range filter.Tags {
			if _, ok := set[tag]; !ok {
				return false
			}
		}
		return true
	}

	matchesType := func(val string) bool {
		if len(filter.Type) == 0 {
			return true
		}
		for _, t := range filter.Type {
			if t == val {
				return true
			}
		}
		return false
	}

	matchesWindow := func(created time.Time) bool {
		if filter.From != nil && created.Before(*filter.From) {
			return false
		}
		if filter.To != nil && created.After(*filter.To) {
			return false
		}
		return true
	}

	summaries := make([]WorkflowSummary, 0, len(ids))
	for _, id := range ids {
		wf := snap.Workflows[id]
		if !matchesState(wf.State) || !matchesTags(wf.Tags) || !matchesType(wf.Type) || !matchesWindow(wf.CreatedAt) {
			continue
		}
		summary, err := r.WorkflowSummary(ctx, id)
		if err != nil {
			continue
		}
		summaries = append(summaries, summary)
	}

	start := filter.Offset
	if start > len(summaries) {
		return []WorkflowSummary{}, nil
	}
	summaries = summaries[start:]
	if filter.Limit > 0 && filter.Limit < len(summaries) {
		summaries = summaries[:filter.Limit]
	}
	return summaries, nil
}

func (r *captureStateReader) ActionHistory(ctx context.Context, id WorkflowID) ([]ActionHistoryRecord, error) {
	_ = ctx
	snap := r.observer.Snapshot()
	actions, ok := snap.Actions[id]
	if !ok {
		return nil, errors.New("workflow not found")
	}
	keys := make([]string, 0, len(actions))
	for key := range actions {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	history := make([]ActionHistoryRecord, 0, len(keys))
	for _, key := range keys {
		record := actions[key]
		if progress := snap.Progress[id][key]; progress.progress != 0 || progress.message != "" {
			record.Progress = progress.progress
			record.Message = progress.message
		}
		history = append(history, record)
	}
	return history, nil
}
