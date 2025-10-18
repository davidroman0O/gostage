-- name: EnqueueWorkflow :exec
INSERT INTO queue_entries (
    id, definition, priority, metadata
) VALUES (
    ?, ?, ?, ?
);
-- name: InsertQueueTags :exec
INSERT INTO queue_entry_tags (entry_id, tag)
VALUES (@entry_id, @tag);

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

-- name: InsertQueueAudit :exec
INSERT INTO queue_audit (
    workflow_id, event, worker_id, attempt, metadata
) VALUES (
    ?, ?, ?, ?, ?
);

-- name: ListQueueAudit :many
SELECT
    workflow_id,
    event,
    worker_id,
    attempt,
    metadata,
    created_at
FROM queue_audit
ORDER BY id DESC
LIMIT ?;

-- name: SelectPendingCandidates :many
SELECT
    qe.id,
    qe.definition,
    qe.priority,
    qe.created_at,
    qe.attempts,
    qe.metadata
FROM queue_entries qe
WHERE qe.state = 'pending'
ORDER BY qe.priority DESC, qe.created_at ASC
LIMIT ? OFFSET ?;

-- name: SelectPendingCandidateTags :many
SELECT
    t.entry_id,
    t.tag
FROM queue_entry_tags t
JOIN (
    SELECT id
    FROM queue_entries
    WHERE state = 'pending'
    ORDER BY priority DESC, created_at ASC
    LIMIT ? OFFSET ?
) AS pending ON pending.id = t.entry_id
ORDER BY t.entry_id, t.tag;

-- name: SelectAllPendingTags :many
SELECT
    qe.id,
    t.tag
FROM queue_entries qe
LEFT JOIN queue_entry_tags t ON t.entry_id = qe.id
WHERE qe.state = 'pending'
ORDER BY qe.priority DESC, qe.created_at ASC, t.tag ASC;

-- name: MarkWorkflowClaimed :one
UPDATE queue_entries
SET state = 'claimed',
    claimed_by = ?2,
    claimed_at = CURRENT_TIMESTAMP,
    lease_id = ?3,
    attempts = attempts + 1
WHERE id = ?1
  AND state = 'pending'
RETURNING attempts;
