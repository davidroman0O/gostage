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
