package testkit

import (
	"context"
	"errors"
	"maps"
	"sort"
	"time"

	"github.com/davidroman0O/gostage/v3/internal/locks"
	state "github.com/davidroman0O/gostage/v3/state"
)

// ProgressEntry captures the latest progress metadata recorded for an action.
type ProgressEntry struct {
	Progress int
	Message  string
}

type WorkflowStatusEvent struct {
	WorkflowID string
	Status     state.WorkflowState
}

type StageStatusEvent struct {
	WorkflowID string
	StageID    string
	Status     state.WorkflowState
}

type ActionStatusEvent struct {
	WorkflowID string
	StageID    string
	ActionID   string
	Status     state.WorkflowState
}

type StageRemovalEvent struct {
	WorkflowID string
	StageID    string
	RemovedBy  string
}

type ActionRemovalEvent struct {
	WorkflowID string
	StageID    string
	ActionID   string
	RemovedBy  string
}

type ActionEventRecord struct {
	WorkflowID string
	StageID    string
	ActionID   string
	Kind       string
	Message    string
	Metadata   map[string]any
}

// CaptureObserver records manager events for use in tests.
type CaptureObserver struct {
	mu locks.Mutex

	workflows map[state.WorkflowID]state.WorkflowRecord
	stages    map[state.WorkflowID]map[string]state.StageRecord
	actions   map[state.WorkflowID]map[string]state.ActionHistoryRecord
	progress  map[state.WorkflowID]map[string]ProgressEntry
	summaries map[state.WorkflowID]state.ResultSummary

	workflowStatuses []WorkflowStatusEvent
	stageStatuses    []StageStatusEvent
	actionStatuses   []ActionStatusEvent
	stageRemovals    []StageRemovalEvent
	actionRemovals   []ActionRemovalEvent
	actionEvents     []ActionEventRecord
}

// Snapshot is a consistent view of the observer state.
type Snapshot struct {
	Workflows map[state.WorkflowID]state.WorkflowRecord
	Stages    map[state.WorkflowID]map[string]state.StageRecord
	Actions   map[state.WorkflowID]map[string]state.ActionHistoryRecord
	Progress  map[state.WorkflowID]map[string]ProgressEntry
	Summaries map[state.WorkflowID]state.ResultSummary

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
		workflows: make(map[state.WorkflowID]state.WorkflowRecord),
		stages:    make(map[state.WorkflowID]map[string]state.StageRecord),
		actions:   make(map[state.WorkflowID]map[string]state.ActionHistoryRecord),
		progress:  make(map[state.WorkflowID]map[string]ProgressEntry),
		summaries: make(map[state.WorkflowID]state.ResultSummary),
	}
}

func (c *CaptureObserver) WorkflowRegistered(_ context.Context, wf state.WorkflowRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workflows[wf.ID] = cloneWorkflowRecord(wf)
}

func (c *CaptureObserver) WorkflowStatus(_ context.Context, workflowID string, status state.WorkflowState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workflowStatuses = append(c.workflowStatuses, WorkflowStatusEvent{WorkflowID: workflowID, Status: status})
	if wf, ok := c.workflows[state.WorkflowID(workflowID)]; ok {
		wf.State = status
		c.workflows[state.WorkflowID(workflowID)] = wf
	}
}

func (c *CaptureObserver) StageRegistered(_ context.Context, workflowID string, stage state.StageRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stages[state.WorkflowID(workflowID)] == nil {
		c.stages[state.WorkflowID(workflowID)] = make(map[string]state.StageRecord)
	}
	c.stages[state.WorkflowID(workflowID)][stage.ID] = cloneStageRecord(stage)
	if wf, ok := c.workflows[state.WorkflowID(workflowID)]; ok {
		if wf.Stages == nil {
			wf.Stages = make(map[string]*state.StageRecord)
		}
		clone := cloneStageRecord(stage)
		wf.Stages[stage.ID] = &clone
		c.workflows[state.WorkflowID(workflowID)] = wf
	}
}

func (c *CaptureObserver) StageStatus(_ context.Context, workflowID, stageID string, status state.WorkflowState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stageStatuses = append(c.stageStatuses, StageStatusEvent{WorkflowID: workflowID, StageID: stageID, Status: status})
	if stages := c.stages[state.WorkflowID(workflowID)]; stages != nil {
		if stage, ok := stages[stageID]; ok {
			stage.Status = status
			stages[stageID] = stage
		}
	}
	if wf, ok := c.workflows[state.WorkflowID(workflowID)]; ok {
		if wf.Stages == nil {
			wf.Stages = make(map[string]*state.StageRecord)
		}
		if stage := wf.Stages[stageID]; stage != nil {
			stage.Status = status
		} else {
			clone := state.StageRecord{ID: stageID, Status: status}
			wf.Stages[stageID] = &clone
		}
		c.workflows[state.WorkflowID(workflowID)] = wf
	}
}

func (c *CaptureObserver) StageRemoved(_ context.Context, workflowID, stageID, removedBy string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stageRemovals = append(c.stageRemovals, StageRemovalEvent{WorkflowID: workflowID, StageID: stageID, RemovedBy: removedBy})
	if stages := c.stages[state.WorkflowID(workflowID)]; stages != nil {
		stage := stages[stageID]
		stage.Status = state.WorkflowRemoved
		stage.CreatedBy = removedBy
		stages[stageID] = stage
	}
}

func (c *CaptureObserver) ActionRegistered(_ context.Context, workflowID, stageID string, action state.ActionRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.actions[state.WorkflowID(workflowID)] == nil {
		c.actions[state.WorkflowID(workflowID)] = make(map[string]state.ActionHistoryRecord)
	}
	key := actionKey(stageID, action.Name)
	c.actions[state.WorkflowID(workflowID)][key] = state.ActionHistoryRecord{
		ActionID:    action.Name,
		StageID:     stageID,
		Ref:         action.Ref,
		Description: action.Description,
		Tags:        append([]string(nil), action.Tags...),
		Dynamic:     action.Dynamic,
		CreatedBy:   action.CreatedBy,
		State:       action.Status,
	}
	if wf, ok := c.workflows[state.WorkflowID(workflowID)]; ok {
		stage := wf.Stages[stageID]
		if stage == nil {
			clone := cloneStageRecord(state.StageRecord{ID: stageID})
			stage = &clone
			if wf.Stages == nil {
				wf.Stages = make(map[string]*state.StageRecord)
			}
			wf.Stages[stageID] = stage
		}
		if stage.Actions == nil {
			stage.Actions = make(map[string]*state.ActionRecord)
		}
		clone := cloneActionRecord(action)
		stage.Actions[action.Name] = &clone
		c.workflows[state.WorkflowID(workflowID)] = wf
	}
}

func (c *CaptureObserver) ActionStatus(_ context.Context, workflowID, stageID, actionID string, status state.WorkflowState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.actionStatuses = append(c.actionStatuses, ActionStatusEvent{WorkflowID: workflowID, StageID: stageID, ActionID: actionID, Status: status})
	if actions := c.actions[state.WorkflowID(workflowID)]; actions != nil {
		key := actionKey(stageID, actionID)
		record := actions[key]
		record.State = status
		actions[key] = record
	}
	if wf, ok := c.workflows[state.WorkflowID(workflowID)]; ok {
		stage := wf.Stages[stageID]
		if stage == nil {
			clone := cloneStageRecord(state.StageRecord{ID: stageID})
			stage = &clone
			if wf.Stages == nil {
				wf.Stages = make(map[string]*state.StageRecord)
			}
			wf.Stages[stageID] = stage
		}
		if stage.Actions == nil {
			stage.Actions = make(map[string]*state.ActionRecord)
		}
		if act := stage.Actions[actionID]; act != nil {
			act.Status = status
		} else {
			stage.Actions[actionID] = &state.ActionRecord{Name: actionID, Status: status}
		}
		c.workflows[state.WorkflowID(workflowID)] = wf
	}
}

func (c *CaptureObserver) ActionRemoved(_ context.Context, workflowID, stageID, actionID, removedBy string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.actionRemovals = append(c.actionRemovals, ActionRemovalEvent{WorkflowID: workflowID, StageID: stageID, ActionID: actionID, RemovedBy: removedBy})
	if actions := c.actions[state.WorkflowID(workflowID)]; actions != nil {
		key := actionKey(stageID, actionID)
		record := actions[key]
		record.State = state.WorkflowRemoved
		actions[key] = record
	}
}

func (c *CaptureObserver) ActionProgress(_ context.Context, workflowID, stageID, actionID string, progress int, message string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.progress[state.WorkflowID(workflowID)] == nil {
		c.progress[state.WorkflowID(workflowID)] = make(map[string]ProgressEntry)
	}
	key := actionKey(stageID, actionID)
	c.progress[state.WorkflowID(workflowID)][key] = ProgressEntry{Progress: progress, Message: message}
}

func (c *CaptureObserver) ActionEvent(_ context.Context, workflowID, stageID, actionID, kind, msg string, metadata map[string]any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var clone map[string]any
	if len(metadata) > 0 {
		clone = maps.Clone(metadata)
	}
	c.actionEvents = append(c.actionEvents, ActionEventRecord{
		WorkflowID: workflowID,
		StageID:    stageID,
		ActionID:   actionID,
		Kind:       kind,
		Message:    msg,
		Metadata:   clone,
	})
}

func (c *CaptureObserver) ExecutionSummary(_ context.Context, workflowID string, summary state.ResultSummary) {
	c.mu.Lock()
	defer c.mu.Unlock()
	clone := cloneResultSummary(summary)
	c.summaries[state.WorkflowID(workflowID)] = clone
}

// Snapshot returns a deep copy of all captured data.
func (c *CaptureObserver) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := Snapshot{
		Workflows:        make(map[state.WorkflowID]state.WorkflowRecord, len(c.workflows)),
		Stages:           make(map[state.WorkflowID]map[string]state.StageRecord, len(c.stages)),
		Actions:          make(map[state.WorkflowID]map[string]state.ActionHistoryRecord, len(c.actions)),
		Progress:         make(map[state.WorkflowID]map[string]ProgressEntry, len(c.progress)),
		Summaries:        make(map[state.WorkflowID]state.ResultSummary, len(c.summaries)),
		WorkflowStatuses: append([]WorkflowStatusEvent(nil), c.workflowStatuses...),
		StageStatuses:    append([]StageStatusEvent(nil), c.stageStatuses...),
		ActionStatuses:   append([]ActionStatusEvent(nil), c.actionStatuses...),
		StageRemovals:    append([]StageRemovalEvent(nil), c.stageRemovals...),
		ActionRemovals:   append([]ActionRemovalEvent(nil), c.actionRemovals...),
		ActionEvents:     append([]ActionEventRecord(nil), c.actionEvents...),
	}

	for id, wf := range c.workflows {
		out.Workflows[id] = cloneWorkflowRecord(wf)
	}
	for id, stages := range c.stages {
		dup := make(map[string]state.StageRecord, len(stages))
		for stageID, stage := range stages {
			dup[stageID] = cloneStageRecord(stage)
		}
		out.Stages[id] = dup
	}
	for id, actions := range c.actions {
		dup := make(map[string]state.ActionHistoryRecord, len(actions))
		for key, record := range actions {
			clone := record
			clone.Tags = append([]string(nil), record.Tags...)
			dup[key] = clone
		}
		out.Actions[id] = dup
	}
	for id, prog := range c.progress {
		dup := make(map[string]ProgressEntry, len(prog))
		for key, entry := range prog {
			dup[key] = entry
		}
		out.Progress[id] = dup
	}
	for id, summary := range c.summaries {
		out.Summaries[id] = cloneResultSummary(summary)
	}
	return out
}

func actionKey(stageID, actionID string) string {
	return stageID + "::" + actionID
}

func cloneWorkflowRecord(rec state.WorkflowRecord) state.WorkflowRecord {
	out := rec
	out.Tags = append([]string(nil), rec.Tags...)
	out.Metadata = maps.Clone(rec.Metadata)
	out.Definition.Metadata = maps.Clone(rec.Definition.Metadata)
	out.Definition.Payload = maps.Clone(rec.Definition.Payload)
	out.Definition.Tags = append([]string(nil), rec.Definition.Tags...)
	if len(rec.Stages) > 0 {
		out.Stages = make(map[string]*state.StageRecord, len(rec.Stages))
		for id, stage := range rec.Stages {
			if stage == nil {
				continue
			}
			clone := cloneStageRecord(*stage)
			out.Stages[id] = &clone
		}
	} else {
		out.Stages = make(map[string]*state.StageRecord)
	}
	return out
}

func cloneStageRecord(rec state.StageRecord) state.StageRecord {
	out := rec
	out.Tags = append([]string(nil), rec.Tags...)
	out.StartedAt = cloneTimePointer(rec.StartedAt)
	out.CompletedAt = cloneTimePointer(rec.CompletedAt)
	if len(rec.Actions) > 0 {
		out.Actions = make(map[string]*state.ActionRecord, len(rec.Actions))
		for id, action := range rec.Actions {
			if action == nil {
				continue
			}
			clone := cloneActionRecord(*action)
			out.Actions[id] = &clone
		}
	} else {
		out.Actions = make(map[string]*state.ActionRecord)
	}
	return out
}

func cloneActionRecord(rec state.ActionRecord) state.ActionRecord {
	out := rec
	out.Tags = append([]string(nil), rec.Tags...)
	out.StartedAt = cloneTimePointer(rec.StartedAt)
	out.CompletedAt = cloneTimePointer(rec.CompletedAt)
	return out
}

func cloneResultSummary(summary state.ResultSummary) state.ResultSummary {
	clone := summary
	clone.Output = maps.Clone(summary.Output)
	clone.DisabledStages = maps.Clone(summary.DisabledStages)
	clone.DisabledActions = maps.Clone(summary.DisabledActions)
	clone.RemovedStages = maps.Clone(summary.RemovedStages)
	clone.RemovedActions = maps.Clone(summary.RemovedActions)
	clone.StageStatuses = cloneStageStatusRecords(summary.StageStatuses)
	clone.ActionStatuses = cloneActionStatusRecords(summary.ActionStatuses)
	return clone
}

func cloneTimePointer(ts *time.Time) *time.Time {
	if ts == nil {
		return nil
	}
	dup := *ts
	return &dup
}

// AwaitWorkflowSummary polls the observer until the workflow exists or a timeout occurs.
func AwaitWorkflowSummary(observer *CaptureObserver, id state.WorkflowID) (state.WorkflowRecord, error) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		snapshot := observer.Snapshot()
		if wf, ok := snapshot.Workflows[id]; ok {
			return wf, nil
		}
		if time.Now().After(deadline) {
			return state.WorkflowRecord{}, errors.New("workflow summary not recorded")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// ListWorkflows sorts workflow IDs by creation time.
func ListWorkflows(observer *CaptureObserver) []state.WorkflowID {
	snapshot := observer.Snapshot()
	ids := make([]state.WorkflowID, 0, len(snapshot.Workflows))
	for id := range snapshot.Workflows {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		left := snapshot.Workflows[ids[i]].CreatedAt
		right := snapshot.Workflows[ids[j]].CreatedAt
		if left.Equal(right) {
			return ids[i] < ids[j]
		}
		return left.Before(right)
	})
	return ids
}

func cloneStageStatusRecords(records []state.StageStatusRecord) []state.StageStatusRecord {
	if len(records) == 0 {
		return nil
	}
	dup := make([]state.StageStatusRecord, len(records))
	for i, rec := range records {
		clone := rec
		clone.Tags = append([]string(nil), rec.Tags...)
		dup[i] = clone
	}
	return dup
}

func cloneActionStatusRecords(records []state.ActionStatusRecord) []state.ActionStatusRecord {
	if len(records) == 0 {
		return nil
	}
	dup := make([]state.ActionStatusRecord, len(records))
	for i, rec := range records {
		clone := rec
		clone.Tags = append([]string(nil), rec.Tags...)
		dup[i] = clone
	}
	return dup
}

// Reader exposes a state.StateReader backed by the captured data.
func (c *CaptureObserver) Reader() state.StateReader {
	return &captureStateReader{observer: c}
}

type captureStateReader struct {
	observer *CaptureObserver
}

func (r *captureStateReader) WorkflowSummary(ctx context.Context, id state.WorkflowID) (state.WorkflowSummary, error) {
	_ = ctx
	snap := r.observer.Snapshot()
	wf, ok := snap.Workflows[id]
	if !ok {
		return state.WorkflowSummary{}, errors.New("workflow not found")
	}
	summary := state.WorkflowSummary{WorkflowRecord: cloneWorkflowRecord(wf)}
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

func (r *captureStateReader) ListWorkflows(ctx context.Context, filter state.StateFilter) ([]state.WorkflowSummary, error) {
	_ = ctx
	snap := r.observer.Snapshot()
	ids := make([]state.WorkflowID, 0, len(snap.Workflows))
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

	matchesState := func(target state.WorkflowState) bool {
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

	summaries := make([]state.WorkflowSummary, 0, len(ids))
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
		return []state.WorkflowSummary{}, nil
	}
	summaries = summaries[start:]
	if filter.Limit > 0 && filter.Limit < len(summaries) {
		summaries = summaries[:filter.Limit]
	}
	return summaries, nil
}

func (r *captureStateReader) ActionHistory(ctx context.Context, id state.WorkflowID) ([]state.ActionHistoryRecord, error) {
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

	history := make([]state.ActionHistoryRecord, 0, len(keys))
	for _, key := range keys {
		record := actions[key]
		if progress := snap.Progress[id][key]; progress.Progress != 0 || progress.Message != "" {
			record.Progress = progress.Progress
			record.Message = progress.Message
		}
		history = append(history, record)
	}
	return history, nil
}
