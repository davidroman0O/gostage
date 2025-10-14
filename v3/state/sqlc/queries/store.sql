-- name: UpsertWorkflowRun :exec
INSERT INTO workflow_runs (
    id, name, description, type, tags, metadata, created_at, state, success, error, started_at, completed_at, duration
) VALUES (
    ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), ?, ?, ?, ?, ?, ?
)
ON CONFLICT(id) DO UPDATE SET
    name = excluded.name,
    description = excluded.description,
    type = excluded.type,
    tags = excluded.tags,
    metadata = excluded.metadata,
    state = excluded.state,
    success = excluded.success,
    error = excluded.error,
    started_at = COALESCE(excluded.started_at, workflow_runs.started_at),
    completed_at = COALESCE(excluded.completed_at, workflow_runs.completed_at),
    duration = COALESCE(excluded.duration, workflow_runs.duration);

-- name: InsertStageRun :exec
INSERT INTO stage_runs (
    workflow_id, stage_id, name, tags, dynamic, created_by, state, started_at, completed_at
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?
)
ON CONFLICT(workflow_id, stage_id) DO UPDATE SET
    name = excluded.name,
    tags = excluded.tags,
    dynamic = excluded.dynamic,
    created_by = excluded.created_by,
    state = excluded.state,
    started_at = COALESCE(excluded.started_at, stage_runs.started_at),
    completed_at = COALESCE(excluded.completed_at, stage_runs.completed_at);

-- name: InsertActionRun :exec
INSERT INTO action_runs (
    workflow_id, stage_id, action_id, ref, tags, dynamic, created_by, state, started_at, completed_at
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)
ON CONFLICT(workflow_id, stage_id, action_id) DO UPDATE SET
    ref = excluded.ref,
    tags = excluded.tags,
    dynamic = excluded.dynamic,
    created_by = excluded.created_by,
    state = excluded.state,
    started_at = COALESCE(excluded.started_at, action_runs.started_at),
    completed_at = COALESCE(excluded.completed_at, action_runs.completed_at);

-- name: UpsertExecutionSummary :exec
INSERT INTO execution_summaries (
    workflow_id, final_store, disabled_stages, disabled_actions, removed_stages, removed_actions
) VALUES (
    ?, ?, ?, ?, ?, ?
)
ON CONFLICT(workflow_id) DO UPDATE SET
    final_store = excluded.final_store,
    disabled_stages = excluded.disabled_stages,
    disabled_actions = excluded.disabled_actions,
    removed_stages = excluded.removed_stages,
    removed_actions = excluded.removed_actions;

-- name: GetWorkflowSummary :one
SELECT id, name, description, type, tags, metadata, created_at, started_at, completed_at, duration, state, success, error
FROM workflow_runs
WHERE id = ?;

-- name: ListAllWorkflows :many
SELECT id, name, description, type, tags, metadata, created_at, started_at, completed_at, duration, state, success, error
FROM workflow_runs
ORDER BY created_at DESC;

-- name: ListActionsByWorkflow :many
SELECT action_id, stage_id, ref, tags, dynamic, created_by, state, started_at, completed_at
FROM action_runs
WHERE workflow_id = ?
ORDER BY created_at ASC;

-- name: GetExecutionSummary :one
SELECT final_store, disabled_stages, disabled_actions, removed_stages, removed_actions
FROM execution_summaries
WHERE workflow_id = ?;
