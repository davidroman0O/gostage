-- name: UpsertWorkflowRun :exec
INSERT INTO workflow_runs (
    id, name, description, type, tags, metadata, created_at, state, success, error, started_at, completed_at, duration, termination_reason
) VALUES (
    ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), ?, ?, ?, ?, ?, ?, ?
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
    duration = COALESCE(excluded.duration, workflow_runs.duration),
    termination_reason = excluded.termination_reason;

-- name: UpdateWorkflowStatus :exec
UPDATE workflow_runs
SET
    state = sqlc.arg(state),
    started_at = COALESCE(sqlc.arg(started_at), workflow_runs.started_at),
    completed_at = COALESCE(sqlc.arg(completed_at), workflow_runs.completed_at),
    duration = COALESCE(sqlc.narg(duration), workflow_runs.duration),
    success = COALESCE(sqlc.narg(success), workflow_runs.success),
    error = COALESCE(sqlc.narg(error), workflow_runs.error),
    termination_reason = COALESCE(sqlc.narg(termination_reason), workflow_runs.termination_reason)
WHERE id = sqlc.arg(id);

-- name: InsertStageRun :exec
INSERT INTO stage_runs (
    workflow_id, stage_id, name, description, tags, dynamic, created_by, state, started_at, completed_at
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)
ON CONFLICT(workflow_id, stage_id) DO UPDATE SET
    name = excluded.name,
    description = excluded.description,
    tags = excluded.tags,
    dynamic = excluded.dynamic,
    created_by = excluded.created_by,
    state = excluded.state,
    started_at = COALESCE(excluded.started_at, stage_runs.started_at),
    completed_at = COALESCE(excluded.completed_at, stage_runs.completed_at);

-- name: UpdateStageStatus :exec
UPDATE stage_runs
SET
    state = sqlc.arg(state),
    started_at = COALESCE(sqlc.arg(started_at), stage_runs.started_at),
    completed_at = COALESCE(sqlc.arg(completed_at), stage_runs.completed_at)
WHERE workflow_id = sqlc.arg(workflow_id)
  AND stage_id = sqlc.arg(stage_id);

-- name: InsertActionRun :exec
INSERT INTO action_runs (
    workflow_id, stage_id, action_id, ref, description, tags, dynamic, created_by, state, started_at, completed_at
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)
ON CONFLICT(workflow_id, stage_id, action_id) DO UPDATE SET
    ref = excluded.ref,
    description = excluded.description,
    tags = excluded.tags,
    dynamic = excluded.dynamic,
    created_by = excluded.created_by,
    state = excluded.state,
    started_at = COALESCE(excluded.started_at, action_runs.started_at),
    completed_at = COALESCE(excluded.completed_at, action_runs.completed_at);

-- name: UpdateActionStatus :exec
UPDATE action_runs
SET
    state = sqlc.arg(state),
    started_at = COALESCE(sqlc.arg(started_at), action_runs.started_at),
    completed_at = COALESCE(sqlc.arg(completed_at), action_runs.completed_at)
WHERE workflow_id = sqlc.arg(workflow_id)
  AND stage_id = sqlc.arg(stage_id)
  AND action_id = sqlc.arg(action_id);

-- name: UpsertExecutionSummary :exec
INSERT INTO execution_summaries (
    workflow_id, final_store, disabled_stages, disabled_actions, removed_stages, removed_actions, termination_reason
) VALUES (
    ?, ?, ?, ?, ?, ?, ?
)
ON CONFLICT(workflow_id) DO UPDATE SET
    final_store = excluded.final_store,
    disabled_stages = excluded.disabled_stages,
    disabled_actions = excluded.disabled_actions,
    removed_stages = excluded.removed_stages,
    removed_actions = excluded.removed_actions,
    termination_reason = excluded.termination_reason;

-- name: GetWorkflowSummary :one
SELECT id, name, description, type, tags, metadata, created_at, started_at, completed_at, duration, state, success, error, termination_reason
FROM workflow_runs
WHERE id = ?;

-- name: ListWorkflowsFiltered :many
SELECT id, name, description, type, tags, metadata, created_at, started_at, completed_at, duration, state, success, error, termination_reason
FROM workflow_runs
WHERE
    (
        sqlc.arg(states_json) IS NULL
        OR EXISTS (
            SELECT 1
            FROM json_each(COALESCE(?1, '[]')) AS state_filter
            WHERE workflow_runs.state = state_filter.value
        )
    )
    AND (
        sqlc.arg(type_json) IS NULL
        OR EXISTS (
            SELECT 1
            FROM json_each(COALESCE(?2, '[]')) AS type_filter
            WHERE workflow_runs.type = type_filter.value
        )
    )
    AND (
        sqlc.arg(tags_json) IS NULL
        OR EXISTS (
            SELECT 1
            FROM json_each(COALESCE(?3, '[]')) AS tag_filter
            WHERE EXISTS (
                SELECT 1
                FROM json_each(workflow_runs.tags) AS wf_tag
                WHERE wf_tag.value = tag_filter.value
            )
        )
    )
    AND (sqlc.arg(from_time) IS NULL OR workflow_runs.created_at >= sqlc.arg(from_time))
    AND (sqlc.arg(to_time) IS NULL OR workflow_runs.created_at <= sqlc.arg(to_time))
ORDER BY created_at DESC
LIMIT CASE WHEN sqlc.arg(limit_rows) IS NULL OR sqlc.arg(limit_rows) <= 0 THEN -1 ELSE sqlc.arg(limit_rows) END
OFFSET COALESCE(sqlc.arg(offset_rows), 0);

-- name: ListActionsByWorkflow :many
SELECT action_id, stage_id, ref, description, tags, dynamic, created_by, state, started_at, completed_at
FROM action_runs
WHERE workflow_id = ?
ORDER BY created_at ASC;

-- name: GetExecutionSummary :one
SELECT final_store, disabled_stages, disabled_actions, removed_stages, removed_actions, termination_reason
FROM execution_summaries
WHERE workflow_id = ?;
