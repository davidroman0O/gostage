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
