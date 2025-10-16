-- name: InsertTelemetryEvent :exec
INSERT INTO telemetry_events (
    workflow_id, stage_id, action_id, kind, attempt, occurred_at, message, metadata, error
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?
);

-- name: ListTelemetryByWorkflow :many
SELECT workflow_id, stage_id, action_id, kind, attempt, occurred_at, message, metadata, error
FROM telemetry_events
WHERE workflow_id = ?
ORDER BY occurred_at ASC;

-- name: ListLatestActionProgress :many
SELECT te.stage_id, te.action_id, te.metadata, te.message
FROM telemetry_events te
JOIN (
    SELECT stage_id, action_id, MAX(id) AS max_id
    FROM telemetry_events tp
    WHERE tp.workflow_id = ? AND tp.kind = 'action.progress'
    GROUP BY tp.stage_id, tp.action_id
) latest ON te.stage_id = latest.stage_id AND te.action_id = latest.action_id AND te.id = latest.max_id
WHERE te.workflow_id = ? AND te.kind = 'action.progress';
