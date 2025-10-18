package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/davidroman0O/gostage/v3/state/sqlc"
	"github.com/davidroman0O/gostage/v3/workflow"
)

// SQLiteQueue implements Queue using the sqlc-generated queries.
type SQLiteQueue struct {
	db      *sql.DB
	queries *sqlc.Queries
	mu      sync.RWMutex
	workers map[WorkflowID]string
}

const pendingCountQuery = `
WITH require_all(tag) AS (
    SELECT value FROM json_each(?)
),
require_any(tag) AS (
    SELECT value FROM json_each(?)
),
exclude(tag) AS (
    SELECT value FROM json_each(?)
)
SELECT COUNT(*) AS pending
FROM queue_entries qe
WHERE qe.state = 'pending'
  AND (
      (SELECT COUNT(*) FROM require_all) = 0
      OR (
          SELECT COUNT(DISTINCT t.tag)
          FROM queue_entry_tags t
          JOIN require_all ra ON ra.tag = t.tag
          WHERE t.entry_id = qe.id
      ) = (SELECT COUNT(*) FROM require_all)
  )
  AND (
      (SELECT COUNT(*) FROM require_any) = 0
      OR EXISTS (
          SELECT 1
          FROM queue_entry_tags t
          JOIN require_any ry ON ry.tag = t.tag
          WHERE t.entry_id = qe.id
      )
  )
  AND NOT EXISTS (
      SELECT 1
      FROM queue_entry_tags t
      JOIN exclude ex ON ex.tag = t.tag
      WHERE t.entry_id = qe.id
  );`

// NewSQLiteQueue instantiates a queue backed by the provided *sql.DB. The caller
// is responsible for running migrations before construction.
func NewSQLiteQueue(db *sql.DB) (*SQLiteQueue, error) {
	if db == nil {
		return nil, errors.New("state: db is nil")
	}
	return &SQLiteQueue{
		db:      db,
		queries: sqlc.New(db),
		workers: make(map[WorkflowID]string),
	}, nil
}

func (q *SQLiteQueue) Enqueue(ctx context.Context, def workflow.Definition, priority Priority, metadata map[string]any) (WorkflowID, error) {
	id := WorkflowID(uuid.NewString())
	payload, err := json.Marshal(def)
	if err != nil {
		return "", err
	}
	metaBytes, err := json.Marshal(metadata)
	if err != nil {
		return "", err
	}
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	qtx := q.queries.WithTx(tx)
	if err := qtx.EnqueueWorkflow(ctx, sqlc.EnqueueWorkflowParams{
		ID:         string(id),
		Definition: payload,
		Priority:   int64(priority),
		Metadata:   metaBytes,
	}); err != nil {
		_ = tx.Rollback()
		return "", err
	}
	for _, tag := range def.Tags {
		if tag == "" {
			continue
		}
		if err := qtx.InsertQueueTags(ctx, sqlc.InsertQueueTagsParams{
			EntryID: string(id),
			Tag:     tag,
		}); err != nil {
			_ = tx.Rollback()
			return "", err
		}
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return id, nil
}

func (q *SQLiteQueue) Claim(ctx context.Context, sel Selector, workerID string) (*ClaimedWorkflow, error) {
	const batchSize int64 = 64

	leaseID := uuid.NewString()
	worker := sql.NullString{String: workerID, Valid: workerID != ""}
	lease := sql.NullString{String: leaseID, Valid: true}

	offset := int64(0)

	for {
		candidates, err := q.queries.SelectPendingCandidates(ctx, sqlc.SelectPendingCandidatesParams{
			Limit:  batchSize,
			Offset: offset,
		})
		if err != nil {
			return nil, err
		}
		if len(candidates) == 0 {
			if offset == 0 {
				return nil, ErrNoPending
			}
			return nil, ErrNoPending
		}

		tagRows, err := q.queries.SelectPendingCandidateTags(ctx, sqlc.SelectPendingCandidateTagsParams{
			Limit:  int64(len(candidates)),
			Offset: offset,
		})
		if err != nil {
			return nil, err
		}
		tagMap := make(map[string][]string, len(candidates))
		for _, row := range tagRows {
			if row.Tag != "" {
				tagMap[row.EntryID] = append(tagMap[row.EntryID], row.Tag)
			}
		}

		var chosen *sqlc.SelectPendingCandidatesRow
		for i := range candidates {
			row := &candidates[i]
			if matchesSelector(tagMap[row.ID], sel) {
				chosen = row
				break
			}
		}

		if chosen == nil {
			if len(candidates) < int(batchSize) {
				return nil, ErrNoPending
			}
			offset += int64(len(candidates))
			continue
		}

		attempts, err := q.queries.MarkWorkflowClaimed(ctx, sqlc.MarkWorkflowClaimedParams{
			ID:        chosen.ID,
			ClaimedBy: worker,
			LeaseID:   lease,
		})
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				offset = 0
				continue
			}
			return nil, err
		}

		var def workflow.Definition
		if err := json.Unmarshal(chosen.Definition, &def); err != nil {
			_ = q.queries.ReleaseWorkflow(ctx, chosen.ID)
			return nil, err
		}

		meta := map[string]any{}
		if len(chosen.Metadata) > 0 {
			if err := json.Unmarshal(chosen.Metadata, &meta); err != nil {
				_ = q.queries.ReleaseWorkflow(ctx, chosen.ID)
				return nil, err
			}
		}

		claimed := &ClaimedWorkflow{
			QueuedWorkflow: QueuedWorkflow{
				ID:         WorkflowID(chosen.ID),
				Definition: def,
				Priority:   Priority(chosen.Priority),
				CreatedAt:  chosen.CreatedAt,
				Attempt:    int(attempts),
				Metadata:   meta,
			},
			LeaseID:   leaseID,
			WorkerID:  workerID,
			ClaimedAt: time.Now(),
		}

		q.setWorker(claimed.ID, workerID)
		q.recordAudit(ctx, claimed.ID, "claim", claimed.Attempt, workerID, map[string]any{
			"selector": map[string][]string{
				"all":  append([]string(nil), sel.All...),
				"any":  append([]string(nil), sel.Any...),
				"none": append([]string(nil), sel.None...),
			},
		})
		return claimed, nil
	}
}

func (q *SQLiteQueue) recordAudit(ctx context.Context, id WorkflowID, event string, attempt int, workerID string, metadata map[string]any) {
	var metaBytes []byte
	if len(metadata) > 0 {
		metaBytes, _ = json.Marshal(metadata)
	}
	worker := sql.NullString{}
	if workerID != "" {
		worker = sql.NullString{String: workerID, Valid: true}
	}
	attemptVal := sql.NullInt64{}
	if attempt > 0 {
		attemptVal = sql.NullInt64{Int64: int64(attempt), Valid: true}
	}
	_ = q.queries.InsertQueueAudit(ctx, sqlc.InsertQueueAuditParams{
		WorkflowID: string(id),
		Event:      event,
		WorkerID:   worker,
		Attempt:    attemptVal,
		Metadata:   metaBytes,
	})
}

func (q *SQLiteQueue) setWorker(id WorkflowID, worker string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if worker == "" {
		delete(q.workers, id)
		return
	}
	q.workers[id] = worker
}

func (q *SQLiteQueue) workerFor(id WorkflowID) string {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.workers[id]
}

func (q *SQLiteQueue) Release(ctx context.Context, id WorkflowID) error {
	worker := q.workerFor(id)
	if err := q.queries.ReleaseWorkflow(ctx, string(id)); err != nil {
		return err
	}
	q.recordAudit(ctx, id, "release", 0, worker, nil)
	q.setWorker(id, "")
	return nil
}

func (q *SQLiteQueue) Ack(ctx context.Context, id WorkflowID, summary ResultSummary) error {
	worker := q.workerFor(id)
	if err := q.queries.AckWorkflow(ctx, string(id)); err != nil {
		return err
	}
	q.recordAudit(ctx, id, "ack", summary.Attempt, worker, nil)
	q.setWorker(id, "")
	return nil
}

func (q *SQLiteQueue) Cancel(ctx context.Context, id WorkflowID) error {
	worker := q.workerFor(id)
	if err := q.queries.CancelWorkflow(ctx, string(id)); err != nil {
		return err
	}
	q.recordAudit(ctx, id, "cancel", 0, worker, nil)
	q.setWorker(id, "")
	return nil
}

func (q *SQLiteQueue) Stats(ctx context.Context) (QueueStats, error) {
	stats, err := q.queries.QueueStats(ctx)
	if err != nil {
		return QueueStats{}, err
	}
	return QueueStats{
		Pending:   int(stats.Pending),
		Claimed:   int(stats.Claimed),
		Cancelled: int(stats.Cancelled),
	}, nil
}

func (q *SQLiteQueue) PendingCount(ctx context.Context, sel Selector) (int, error) {
	mustJSON := func(tags []string) (string, error) {
		if len(tags) == 0 {
			return "[]", nil
		}
		data, err := json.Marshal(tags)
		if err != nil {
			return "", err
		}
		return string(data), nil
	}

	requireAll, err := mustJSON(sel.All)
	if err != nil {
		return 0, err
	}
	requireAny, err := mustJSON(sel.Any)
	if err != nil {
		return 0, err
	}
	exclude, err := mustJSON(sel.None)
	if err != nil {
		return 0, err
	}

	row := q.db.QueryRowContext(ctx, pendingCountQuery, requireAll, requireAny, exclude)
	var total int
	if err := row.Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func (q *SQLiteQueue) AuditLog(ctx context.Context, limit int) ([]QueueAuditRecord, error) {
	if limit <= 0 {
		limit = DefaultAuditLogLimit
	}
	rows, err := q.queries.ListQueueAudit(ctx, int64(limit))
	if err != nil {
		return nil, err
	}
	records := make([]QueueAuditRecord, len(rows))
	for i, row := range rows {
		rec := QueueAuditRecord{
			WorkflowID: WorkflowID(row.WorkflowID),
			Event:      row.Event,
			Timestamp:  row.CreatedAt,
		}
		if row.WorkerID.Valid {
			rec.WorkerID = row.WorkerID.String
		}
		if row.Attempt.Valid {
			rec.Attempt = int(row.Attempt.Int64)
		}
		if len(row.Metadata) > 0 {
			var meta map[string]any
			if err := json.Unmarshal(row.Metadata, &meta); err == nil {
				rec.Metadata = meta
			}
		}
		records[i] = rec
	}
	return records, nil
}

func (q *SQLiteQueue) Close() error {
	return nil
}

func matchesSelector(tags []string, sel Selector) bool {
	if len(sel.All) == 0 && len(sel.Any) == 0 && len(sel.None) == 0 {
		return true
	}
	set := make(map[string]struct{}, len(tags))
	for _, tag := range tags {
		set[tag] = struct{}{}
	}
	for _, required := range sel.All {
		if _, ok := set[required]; !ok {
			return false
		}
	}
	if len(sel.Any) > 0 {
		match := false
		for _, opt := range sel.Any {
			if _, ok := set[opt]; ok {
				match = true
				break
			}
		}
		if !match {
			return false
		}
	}
	for _, excluded := range sel.None {
		if _, ok := set[excluded]; ok {
			return false
		}
	}
	return true
}
