package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/davidroman0O/gostage/v2/state"
	deadlock "github.com/sasha-s/go-deadlock"
)

var (
	errWorkflowExists   = errors.New("state: workflow already exists")
	errWorkflowNotFound = errors.New("state: workflow not found")
	errWorkerExists     = errors.New("state: worker already registered")
	errWorkerNotFound   = errors.New("state: worker not found")
	errNoWorkAvailable  = errors.New("state: no work available")
)

type Manager struct {
	mu        deadlock.RWMutex
	workflows map[string]*workflowEntry
	queue     *priorityQueue
	claimed   map[string]*workflowEntry
	workers   map[string]*state.WorkerState
	events    map[state.EventType][]func(state.Event)
	waiters   map[string][]chan *state.WorkflowResult
	closed    bool
	cond      *sync.Cond
	idCounter uint64
	summaries map[string]state.ExecutionReport
}

func New() *Manager {
	queue := newPriorityQueue()
	m := &Manager{
		workflows: make(map[string]*workflowEntry),
		queue:     queue,
		claimed:   make(map[string]*workflowEntry),
		workers:   make(map[string]*state.WorkerState),
		events:    make(map[state.EventType][]func(state.Event)),
		waiters:   make(map[string][]chan *state.WorkflowResult),
		summaries: make(map[string]state.ExecutionReport),
	}
	m.cond = sync.NewCond(&m.mu)
	return m
}

type workflowEntry struct {
	definition state.SubWorkflowDef
	status     state.WorkflowStatus
	result     *state.WorkflowResult
	stages     map[string]*stageEntry
}

type stageEntry struct {
	record  state.StageRecord
	actions map[string]*state.ActionRecord
}

// priority queue implementation -------------------------------------------

type queueItem struct {
	entry    *workflowEntry
	priority state.Priority
	enqueued time.Time
}

type priorityQueue struct {
	items []*queueItem
}

func newPriorityQueue() *priorityQueue {
	return &priorityQueue{items: make([]*queueItem, 0)}
}

func (pq *priorityQueue) len() int { return len(pq.items) }

func (pq *priorityQueue) push(entry *workflowEntry, priority state.Priority) {
	item := &queueItem{entry: entry, priority: priority, enqueued: time.Now()}
	insert := len(pq.items)
	pq.items = append(pq.items, nil)
	for insert > 0 {
		prev := pq.items[insert-1]
		if higherPriority(prev, item) {
			break
		}
		if insert < len(pq.items) {
			pq.items[insert] = prev
		}
		insert--
	}
	pq.items[insert] = item
}

func (pq *priorityQueue) popMatching(filter state.WorkflowFilter) *workflowEntry {
	idx := pq.findIndex(filter)
	if idx < 0 {
		return nil
	}
	item := pq.items[idx]
	pq.items = append(pq.items[:idx], pq.items[idx+1:]...)
	return item.entry
}

func (pq *priorityQueue) peekMatching(filter state.WorkflowFilter) *workflowEntry {
	idx := pq.findIndex(filter)
	if idx < 0 {
		return nil
	}
	return pq.items[idx].entry
}

func (pq *priorityQueue) each(fn func(*workflowEntry)) {
	for _, item := range pq.items {
		fn(item.entry)
	}
}

func (pq *priorityQueue) remove(workflowID string) {
	for i, item := range pq.items {
		if item.entry.status.ID == workflowID {
			pq.items = append(pq.items[:i], pq.items[i+1:]...)
			return
		}
	}
}

func (pq *priorityQueue) findIndex(filter state.WorkflowFilter) int {
	for i, item := range pq.items {
		if matchesType(item.entry.definition.Type, filter.Types) && containsTags(item.entry.definition.Tags, filter.Tags) {
			return i
		}
	}
	if len(filter.Types) == 0 && len(filter.Tags) == 0 && len(pq.items) > 0 {
		return 0
	}
	return -1
}

func higherPriority(a, b *queueItem) bool {
	if a.priority == b.priority {
		return a.enqueued.Before(b.enqueued)
	}
	return a.priority > b.priority
}

// Map helpers -------------------------------------------------------------

func cloneMap(src map[string]interface{}) map[string]interface{} {
	if src == nil {
		return nil
	}
	copy := make(map[string]interface{}, len(src))
	for k, v := range src {
		copy[k] = v
	}
	return copy
}

func cloneBoolMap(src map[string]bool) map[string]bool {
	if src == nil {
		return nil
	}
	copy := make(map[string]bool, len(src))
	for k, v := range src {
		copy[k] = v
	}
	return copy
}

func cloneStringMap(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	copy := make(map[string]string, len(src))
	for k, v := range src {
		copy[k] = v
	}
	return copy
}

func cloneExecutionReport(report state.ExecutionReport) state.ExecutionReport {
	cloned := report
	cloned.WorkflowTags = append([]string(nil), report.WorkflowTags...)
	cloned.FinalStore = cloneMap(report.FinalStore)
	cloned.DisabledStages = cloneBoolMap(report.DisabledStages)
	cloned.DisabledActions = cloneBoolMap(report.DisabledActions)
	cloned.RemovedStages = cloneStringMap(report.RemovedStages)
	cloned.RemovedActions = cloneStringMap(report.RemovedActions)
	cloned.Stages = cloneStageSummaries(report.Stages)
	return cloned
}

func cloneStageSummaries(stages []state.StageSummary) []state.StageSummary {
	if stages == nil {
		return nil
	}
	cloned := make([]state.StageSummary, len(stages))
	for i, stage := range stages {
		copyStage := stage
		copyStage.Tags = append([]string(nil), stage.Tags...)
		copyStage.Actions = cloneActionSummaries(stage.Actions)
		cloned[i] = copyStage
	}
	return cloned
}

func cloneActionSummaries(actions []state.ActionSummary) []state.ActionSummary {
	if actions == nil {
		return nil
	}
	cloned := make([]state.ActionSummary, len(actions))
	for i, action := range actions {
		copyAction := action
		copyAction.Tags = append([]string(nil), action.Tags...)
		cloned[i] = copyAction
	}
	return cloned
}

// Close releases waiters and marks the manager as closed.
func (m *Manager) Close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	for id, chans := range m.waiters {
		for _, ch := range chans {
			close(ch)
		}
		delete(m.waiters, id)
	}
	m.cond.Broadcast()
	m.mu.Unlock()
}

// WorkflowRegistered records workflow metadata provided by the runner/broker.
func (m *Manager) WorkflowRegistered(ctx context.Context, wf state.WorkflowRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, exists := m.workflows[wf.ID]
	if !exists {
		stageMap := make(map[string]*stageEntry)
		status := state.WorkflowStatus{
			ID:         wf.ID,
			Definition: wf.Definition,
			State:      wf.State,
			Stages:     make(map[string]*state.StageRecord),
		}
		if wf.Stages != nil {
			for id, stageRec := range wf.Stages {
				if stageRec == nil {
					continue
				}
				entryStage := newStageEntry(*stageRec)
				stageMap[id] = entryStage
				status.Stages[id] = &entryStage.record
			}
		}
		entry = &workflowEntry{
			definition: wf.Definition,
			status:     status,
			stages:     stageMap,
		}
		m.workflows[wf.ID] = entry
		return nil
	}

	entry.definition = wf.Definition
	entry.status.Definition = wf.Definition
	entry.status.State = wf.State
	return nil
}

// WorkflowStatus updates the high-level workflow state.
func (m *Manager) WorkflowStatus(ctx context.Context, workflowID string, status state.WorkflowState) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.workflows[workflowID]
	if !ok {
		return errWorkflowNotFound
	}
	entry.status.State = status
	if status == state.WorkflowCompleted || status == state.WorkflowFailed || status == state.WorkflowCancelled || status == state.WorkflowSkipped || status == state.WorkflowRemoved {
		entry.status.EndedAt = time.Now()
	}
	return nil
}

// StageRegistered registers metadata for a stage.
func (m *Manager) StageRegistered(ctx context.Context, workflowID string, stage state.StageRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.workflows[workflowID]
	if !ok {
		return errWorkflowNotFound
	}
	if entry.stages == nil {
		entry.stages = make(map[string]*stageEntry)
	}
	entry.stages[stage.ID] = newStageEntry(stage)
	return nil
}

// StageStatus updates stage state information.
func (m *Manager) StageStatus(ctx context.Context, workflowID, stageID string, status state.WorkflowState) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.workflows[workflowID]
	if !ok {
		return errWorkflowNotFound
	}
	stageEntry := entry.ensureStage(stageID)
	stageEntry.record.Status = status
	if status == state.WorkflowRunning {
		stageEntry.record.StartedAt = time.Now()
	}
	if status == state.WorkflowCompleted || status == state.WorkflowFailed || status == state.WorkflowCancelled || status == state.WorkflowSkipped || status == state.WorkflowRemoved {
		stageEntry.record.EndedAt = time.Now()
	}
	return nil
}

// ActionRegistered records action metadata.
func (m *Manager) ActionRegistered(ctx context.Context, workflowID, stageID string, action state.ActionRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.workflows[workflowID]
	if !ok {
		return errWorkflowNotFound
	}
	stageEntry := entry.ensureStage(stageID)
	if stageEntry.actions == nil {
		stageEntry.actions = make(map[string]*state.ActionRecord)
	}
	copy := action
	stageEntry.actions[action.Name] = &copy
	return nil
}

// ActionStatus updates action state.
func (m *Manager) ActionStatus(ctx context.Context, workflowID, stageID, actionName string, status state.WorkflowState) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.workflows[workflowID]
	if !ok {
		return errWorkflowNotFound
	}
	stageEntry := entry.ensureStage(stageID)
	actionEntry := stageEntry.ensureAction(actionName)
	actionEntry.Status = status
	if status == state.WorkflowRunning {
		actionEntry.StartedAt = time.Now()
	}
	if status == state.WorkflowCompleted || status == state.WorkflowFailed || status == state.WorkflowCancelled || status == state.WorkflowSkipped || status == state.WorkflowRemoved {
		actionEntry.EndedAt = time.Now()
	}
	return nil
}

// ActionProgress updates action progress metrics.
func (m *Manager) ActionProgress(ctx context.Context, workflowID, stageID, actionName string, progress int, message string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.workflows[workflowID]
	if !ok {
		return errWorkflowNotFound
	}
	stageEntry := entry.ensureStage(stageID)
	actionEntry := stageEntry.ensureAction(actionName)
	actionEntry.Progress = progress
	actionEntry.Message = message
	return nil
}

// ActionRemoved records removal metadata for an action.
func (m *Manager) ActionRemoved(ctx context.Context, workflowID, stageID, actionName, createdBy string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.workflows[workflowID]
	if !ok {
		return errWorkflowNotFound
	}
	stageEntry := entry.ensureStage(stageID)
	actionEntry := stageEntry.ensureAction(actionName)
	actionEntry.Status = state.WorkflowRemoved
	actionEntry.EndedAt = time.Now()
	actionEntry.RemovedBy = createdBy
	return nil
}

// StageRemoved records removal metadata for a stage.
func (m *Manager) StageRemoved(ctx context.Context, workflowID, stageID, createdBy string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.workflows[workflowID]
	if !ok {
		return errWorkflowNotFound
	}
	stageEntry := entry.ensureStage(stageID)
	stageEntry.record.Status = state.WorkflowRemoved
	stageEntry.record.EndedAt = time.Now()
	stageEntry.record.RemovedBy = createdBy
	return nil
}

// StoreExecutionSummary persists the final execution report for the workflow.
func (m *Manager) StoreExecutionSummary(ctx context.Context, workflowID string, report state.ExecutionReport) error {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return errors.New("state: manager closed")
	}
	if _, ok := m.workflows[workflowID]; !ok {
		return errWorkflowNotFound
	}
	m.summaries[workflowID] = cloneExecutionReport(report)
	return nil
}

// GetExecutionSummary retrieves a stored execution report for the workflow.
func (m *Manager) GetExecutionSummary(ctx context.Context, workflowID string) (*state.ExecutionReport, error) {
	_ = ctx
	m.mu.RLock()
	defer m.mu.RUnlock()

	report, ok := m.summaries[workflowID]
	if !ok {
		return nil, errWorkflowNotFound
	}
	cloned := cloneExecutionReport(report)
	return &cloned, nil
}

// Store enqueues a workflow with default priority.
func (m *Manager) Store(def state.SubWorkflowDef) string {
	return m.StoreWithPriority(def, def.Priority)
}

// StoreWithPriority enqueues a workflow with explicit priority.
func (m *Manager) StoreWithPriority(def state.SubWorkflowDef, priority state.Priority) string {
	m.mu.Lock()
	defer m.mu.Unlock()

	if def.ID == "" {
		def.ID = m.GenerateWorkflowID()
	}
	if _, exists := m.workflows[def.ID]; exists {
		return ""
	}
	if def.CreatedAt.IsZero() {
		def.CreatedAt = time.Now()
	}
	def.Priority = priority

	status := state.WorkflowStatus{
		ID:         def.ID,
		Definition: def,
		State:      state.WorkflowPending,
		Stages:     make(map[string]*state.StageRecord),
	}
	entry := &workflowEntry{
		definition: def,
		status:     status,
		stages:     make(map[string]*stageEntry),
	}
	m.workflows[def.ID] = entry
	m.queue.push(entry, priority)
	m.cond.Broadcast()
	m.notifyEventLocked(state.EventWorkflowStored, entry.snapshot())
	return def.ID
}

// ClaimWorkflow attempts to claim a workflow matching the filter.
func (m *Manager) ClaimWorkflow(workerID, workerType string, filter state.WorkflowFilter) (*state.QueuedWorkflow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil, errors.New("state: manager closed")
	}

	entry := m.queue.popMatching(filter)
	if entry == nil {
		return nil, errNoWorkAvailable
	}
	entry.status.State = state.WorkflowClaimed
	entry.status.WorkerID = workerID
	entry.status.WorkerType = workerType
	entry.status.ClaimedAt = time.Now()
	m.claimed[entry.status.ID] = entry
	m.notifyEventLocked(state.EventWorkflowClaimed, entry.snapshot())
	return &state.QueuedWorkflow{Status: entry.snapshot()}, nil
}

// ReleaseWorkflow returns a claimed workflow to the pending queue.
func (m *Manager) ReleaseWorkflow(workflowID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.workflows[workflowID]
	if !ok {
		return errWorkflowNotFound
	}
	entry.status.State = state.WorkflowPending
	entry.status.WorkerID = ""
	entry.status.WorkerType = ""
	entry.status.ClaimedAt = time.Time{}
	delete(m.claimed, workflowID)
	m.queue.push(entry, entry.definition.Priority)
	m.cond.Broadcast()
	return nil
}

// CompleteWorkflow marks a workflow as completed and stores the result.
func (m *Manager) CompleteWorkflow(workflowID string, result *state.WorkflowResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.workflows[workflowID]
	if !ok {
		return errWorkflowNotFound
	}
	entry.result = result
	entry.status.State = state.WorkflowCompleted
	entry.status.EndedAt = time.Now()
	entry.status.Result = result
	delete(m.claimed, workflowID)
	m.resolveWaitersLocked(workflowID, result)
	m.notifyEventLocked(state.EventWorkflowUpdated, entry.snapshot())
	return nil
}

// CancelWorkflow marks the workflow as cancelled.
func (m *Manager) CancelWorkflow(workflowID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.workflows[workflowID]
	if !ok {
		return errWorkflowNotFound
	}
	entry.status.State = state.WorkflowCancelled
	entry.status.EndedAt = time.Now()
	delete(m.claimed, workflowID)
	m.queue.remove(workflowID)
	m.resolveWaitersLocked(workflowID, &state.WorkflowResult{Success: false, Error: "cancelled", EndedAt: time.Now()})
	m.notifyEventLocked(state.EventWorkflowUpdated, entry.snapshot())
	return nil
}

// Pull returns a pending workflow without claiming it.
func (m *Manager) Pull(filter state.WorkflowFilter) (*state.QueuedWorkflow, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry := m.queue.peekMatching(filter)
	if entry == nil {
		return nil, errNoWorkAvailable
	}
	return &state.QueuedWorkflow{Status: entry.snapshot()}, nil
}

// GetStatus returns the current workflow status snapshot.
func (m *Manager) GetStatus(workflowID string) (*state.WorkflowStatus, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.workflows[workflowID]
	if !ok {
		return nil, errWorkflowNotFound
	}
	status := entry.snapshot()
	return &status, nil
}

// WaitForCompletion waits for a workflow to complete or timeout.
func (m *Manager) WaitForCompletion(workflowID string, timeout time.Duration) (*state.WorkflowResult, error) {
	m.mu.Lock()
	entry, ok := m.workflows[workflowID]
	if !ok {
		m.mu.Unlock()
		return nil, errWorkflowNotFound
	}
	if entry.result != nil {
		res := *entry.result
		m.mu.Unlock()
		return &res, nil
	}
	ch := make(chan *state.WorkflowResult, 1)
	m.waiters[workflowID] = append(m.waiters[workflowID], ch)
	m.mu.Unlock()

	select {
	case res, ok := <-ch:
		if !ok {
			return nil, errors.New("state: manager closed")
		}
		if res == nil {
			return &state.WorkflowResult{Success: false, Error: "cancelled"}, nil
		}
		copy := *res
		return &copy, nil
	case <-time.After(timeout):
		return nil, errors.New("state: wait timeout")
	}
}

// QueryWorkflows returns workflows matching the query.
func (m *Manager) QueryWorkflows(query state.WorkflowQuery) []*state.WorkflowStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var results []*state.WorkflowStatus
	for _, entry := range m.workflows {
		if !matchesState(entry.status.State, query.States) {
			continue
		}
		if !matchesType(entry.definition.Type, query.Types) {
			continue
		}
		if !containsTags(entry.definition.Tags, query.Tags) {
			continue
		}
		status := entry.snapshot()
		results = append(results, &status)
	}
	return results
}

// GetWorkflowCounts returns counts per state.
func (m *Manager) GetWorkflowCounts() map[state.WorkflowState]int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	counts := make(map[state.WorkflowState]int)
	for _, entry := range m.workflows {
		counts[entry.status.State]++
	}
	return counts
}

// ListPendingWorkflows returns queue snapshot.
func (m *Manager) ListPendingWorkflows() []*state.QueuedWorkflow {
	m.mu.RLock()
	defer m.mu.RUnlock()
	items := make([]*state.QueuedWorkflow, 0, m.queue.len())
	m.queue.each(func(entry *workflowEntry) {
		items = append(items, &state.QueuedWorkflow{Status: entry.snapshot()})
	})
	return items
}

func (m *Manager) resolveWaitersLocked(workflowID string, result *state.WorkflowResult) {
	if chans, ok := m.waiters[workflowID]; ok {
		for _, ch := range chans {
			ch <- result
			close(ch)
		}
		delete(m.waiters, workflowID)
	}
}

func (m *Manager) notifyEventLocked(eventType state.EventType, payload interface{}) {
	handlers := append([]func(state.Event){}, m.events[eventType]...)
	if len(handlers) == 0 {
		return
	}
	event := state.Event{Type: eventType, Time: time.Now(), Payload: payload}
	go func() {
		for _, handler := range handlers {
			handler(event)
		}
	}()
}

// Helper predicates -------------------------------------------------------

func matchesState(current state.WorkflowState, states []state.WorkflowState) bool {
	if len(states) == 0 {
		return true
	}
	for _, st := range states {
		if st == current {
			return true
		}
	}
	return false
}

func matchesType(current string, types []string) bool {
	if len(types) == 0 {
		return true
	}
	for _, typ := range types {
		if typ == current {
			return true
		}
	}
	return false
}

func containsTags(current []string, tags []string) bool {
	if len(tags) == 0 {
		return true
	}
	set := make(map[string]struct{}, len(current))
	for _, tag := range current {
		set[tag] = struct{}{}
	}
	for _, tag := range tags {
		if _, ok := set[tag]; !ok {
			return false
		}
	}
	return true
}

func newStageEntry(rec state.StageRecord) *stageEntry {
	copy := rec
	copy.Tags = append([]string(nil), rec.Tags...)
	actions := make(map[string]*state.ActionRecord)
	if rec.Actions != nil {
		for name, act := range rec.Actions {
			ac := *act
			ac.Tags = append([]string(nil), act.Tags...)
			actions[name] = &ac
		}
	}
	copy.Actions = nil
	return &stageEntry{record: copy, actions: actions}
}

func (e *workflowEntry) snapshot() state.WorkflowStatus {
	status := e.status
	status.Definition.Tags = append([]string(nil), e.definition.Tags...)
	status.Definition.Metadata = cloneMap(e.definition.Metadata)
	status.Definition.Payload = cloneMap(e.definition.Payload)
	status.Stages = make(map[string]*state.StageRecord, len(e.stages))
	for id, stage := range e.stages {
		stageCopy := stage.record
		stageCopy.Tags = append([]string(nil), stageCopy.Tags...)
		stageCopy.Actions = make(map[string]*state.ActionRecord, len(stage.actions))
		for name, action := range stage.actions {
			actionCopy := *action
			actionCopy.Tags = append([]string(nil), action.Tags...)
			stageCopy.Actions[name] = &actionCopy
		}
		status.Stages[id] = &stageCopy
	}
	return status
}

func (e *workflowEntry) ensureStage(stageID string) *stageEntry {
	if e.stages == nil {
		e.stages = make(map[string]*stageEntry)
	}
	stage, ok := e.stages[stageID]
	if !ok {
		stage = &stageEntry{
			record: state.StageRecord{ID: stageID, Actions: make(map[string]*state.ActionRecord)},
		}
		e.stages[stageID] = stage
	}
	if stage.actions == nil {
		stage.actions = make(map[string]*state.ActionRecord)
	}
	if stage.record.Actions == nil {
		stage.record.Actions = make(map[string]*state.ActionRecord)
	}
	if e.status.Stages == nil {
		e.status.Stages = make(map[string]*state.StageRecord)
	}
	e.status.Stages[stageID] = &stage.record
	return stage
}

func (se *stageEntry) ensureAction(actionName string) *state.ActionRecord {
	action, ok := se.actions[actionName]
	if !ok {
		action = &state.ActionRecord{Name: actionName}
		se.actions[actionName] = action
	}
	se.record.Actions[actionName] = action
	return action
}

// ListClaimedWorkflows returns claimed workflows snapshot.
func (m *Manager) ListClaimedWorkflows() []*state.ClaimedWorkflow {
	m.mu.RLock()
	defer m.mu.RUnlock()
	items := make([]*state.ClaimedWorkflow, 0, len(m.claimed))
	for _, entry := range m.claimed {
		items = append(items, &state.ClaimedWorkflow{Status: entry.snapshot()})
	}
	return items
}

// RegisterWorker records worker metadata.
func (m *Manager) RegisterWorker(workerID, workerType string, tags []string, metadata map[string]interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.workers[workerID]; exists {
		return errWorkerExists
	}
	copyTags := append([]string(nil), tags...)
	copyMetadata := cloneMap(metadata)
	m.workers[workerID] = &state.WorkerState{
		ID:         workerID,
		Type:       workerType,
		Tags:       copyTags,
		Metadata:   copyMetadata,
		Status:     state.WorkerIdle,
		LastBeatAt: time.Now(),
	}
	return nil
}

// UnregisterWorker removes worker metadata.
func (m *Manager) UnregisterWorker(workerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.workers[workerID]; !exists {
		return errWorkerNotFound
	}
	delete(m.workers, workerID)
	return nil
}

// WorkerHeartbeat updates heartbeat timestamp.
func (m *Manager) WorkerHeartbeat(workerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	worker, ok := m.workers[workerID]
	if !ok {
		return errWorkerNotFound
	}
	worker.LastBeatAt = time.Now()
	return nil
}

// UpdateWorkerState updates the worker status.
func (m *Manager) UpdateWorkerState(workerID string, status state.WorkerStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	worker, ok := m.workers[workerID]
	if !ok {
		return errWorkerNotFound
	}
	worker.Status = status
	return nil
}

// ListWorkers returns worker snapshots.
func (m *Manager) ListWorkers() []*state.WorkerState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	items := make([]*state.WorkerState, 0, len(m.workers))
	for _, worker := range m.workers {
		copy := *worker
		copy.Tags = append([]string(nil), worker.Tags...)
		copy.Metadata = cloneMap(worker.Metadata)
		items = append(items, &copy)
	}
	return items
}

// GetWorkerCounts returns counts per worker state.
func (m *Manager) GetWorkerCounts() map[state.WorkerStatus]int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	counts := make(map[state.WorkerStatus]int)
	for _, worker := range m.workers {
		counts[worker.Status]++
	}
	return counts
}

// WaitForWork blocks until work is available or timeout expires.
func (m *Manager) WaitForWork(workerType string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	m.mu.Lock()
	for {
		if m.closed {
			m.mu.Unlock()
			return errors.New("state: manager closed")
		}
		if entry := m.queue.peekMatching(state.WorkflowFilter{Types: []string{workerType}}); entry != nil {
			m.mu.Unlock()
			return nil
		}
		if timeout > 0 && time.Now().After(deadline) {
			m.mu.Unlock()
			return errNoWorkAvailable
		}
		m.cond.Wait()
	}
}

// AddEventHandler registers a handler for the given event type.
func (m *Manager) AddEventHandler(eventType state.EventType, handler func(state.Event)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events[eventType] = append(m.events[eventType], handler)
}

// RemoveEventHandlers removes all handlers for an event type.
func (m *Manager) RemoveEventHandlers(eventType state.EventType) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.events, eventType)
}

// GenerateWorkflowID returns a unique workflow identifier.
func (m *Manager) GenerateWorkflowID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.idCounter++
	return fmt.Sprintf("wf-%d", m.idCounter)
}
