-- name: EnqueueWorkflow :exec
INSERT INTO queue_entries (
    id, definition, priority, metadata
) VALUES (
    ?, ?, ?, ?
);

-- name: ClaimNextWorkflow :one
UPDATE queue_entries
SET state = 'claimed',
    claimed_by = ?,
    claimed_at = CURRENT_TIMESTAMP,
    lease_id = ?,
    attempts = attempts + 1
WHERE id = (
    SELECT id FROM queue_entries
    WHERE state = 'pending'
    ORDER BY priority DESC, created_at ASC
    LIMIT 1
)
RETURNING id, definition, priority, created_at, attempts, claimed_by, claimed_at, lease_id, metadata;

-- name: ReleaseWorkflow :exec
UPDATE queue_entries
SET state = 'pending',
    claimed_by = NULL,
    claimed_at = NULL,
    lease_id = NULL
WHERE id = ?;

-- name: AckWorkflow :exec
DELETE FROM queue_entries
WHERE id = ?;

-- name: CancelWorkflow :exec
UPDATE queue_entries
SET state = 'cancelled'
WHERE id = ?;

-- name: QueueStats :one
SELECT
    (SELECT COUNT(*) FROM queue_entries WHERE state = 'pending') AS pending,
    (SELECT COUNT(*) FROM queue_entries WHERE state = 'claimed') AS claimed,
    (SELECT COUNT(*) FROM queue_entries WHERE state = 'cancelled') AS cancelled;
