package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/davidroman0O/gostage/v3/state/sqlc"
	"github.com/davidroman0O/gostage/v3/workflow"
)

// SQLiteQueue implements Queue using the sqlc-generated queries.
type SQLiteQueue struct {
	db      *sql.DB
	queries *sqlc.Queries
}

// NewSQLiteQueue instantiates a queue backed by the provided *sql.DB. The caller
// is responsible for running migrations before construction.
func NewSQLiteQueue(db *sql.DB) (*SQLiteQueue, error) {
	if db == nil {
		return nil, errors.New("state: db is nil")
	}
	return &SQLiteQueue{db: db, queries: sqlc.New(db)}, nil
}

const claimNextWorkflowSQL = `
UPDATE queue_entries
SET state = 'claimed',
    claimed_by = @claimed_by,
    claimed_at = CURRENT_TIMESTAMP,
    lease_id = @lease_id,
    attempts = attempts + 1
WHERE id = (
    SELECT qe.id
    FROM queue_entries qe
    WHERE qe.state = 'pending'
      AND NOT EXISTS (
          SELECT 1
          FROM json_each(@required_tags) rt
          WHERE NOT EXISTS (
              SELECT 1
              FROM queue_entry_tags t
              WHERE t.entry_id = qe.id AND t.tag = rt.value
          )
      )
      AND (
          NOT EXISTS (SELECT 1 FROM json_each(@any_tags))
          OR EXISTS (
              SELECT 1
              FROM queue_entry_tags t
              JOIN json_each(@any_tags) at ON at.value = t.tag
              WHERE t.entry_id = qe.id
          )
      )
      AND NOT EXISTS (
          SELECT 1
          FROM queue_entry_tags t
          JOIN json_each(@none_tags) nt ON nt.value = t.tag
          WHERE t.entry_id = qe.id
      )
    ORDER BY qe.priority DESC, qe.created_at ASC
    LIMIT 1
)
RETURNING id, definition, priority, created_at, attempts, claimed_by, claimed_at, lease_id, metadata;
`

const pendingCountSQL = `
SELECT COUNT(*) AS pending
FROM queue_entries qe
WHERE qe.state = 'pending'
  AND NOT EXISTS (
      SELECT 1
      FROM json_each(@required_tags) rt
      WHERE NOT EXISTS (
          SELECT 1
          FROM queue_entry_tags t
          WHERE t.entry_id = qe.id AND t.tag = rt.value
      )
  )
  AND (
      NOT EXISTS (SELECT 1 FROM json_each(@any_tags))
      OR EXISTS (
          SELECT 1
          FROM queue_entry_tags t
          JOIN json_each(@any_tags) at ON at.value = t.tag
          WHERE t.entry_id = qe.id
      )
  )
  AND NOT EXISTS (
      SELECT 1
      FROM queue_entry_tags t
      JOIN json_each(@none_tags) nt ON nt.value = t.tag
      WHERE t.entry_id = qe.id
  );
`

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
	leaseID := uuid.NewString()
	worker := sql.NullString{String: workerID, Valid: workerID != ""}
	lease := sql.NullString{String: leaseID, Valid: true}

	params := []any{
		sql.Named("required_tags", string(encodeStringSlice(sel.All))),
		sql.Named("any_tags", string(encodeStringSlice(sel.Any))),
		sql.Named("none_tags", string(encodeStringSlice(sel.None))),
		sql.Named("claimed_by", worker),
		sql.Named("lease_id", lease),
	}

	var (
		id        string
		defBytes  []byte
		metaBytes []byte
		priority  int64
		createdAt time.Time
		attempts  int64
		claimedBy sql.NullString
		claimedAt sql.NullTime
		leaseVal  sql.NullString
	)

	row := q.db.QueryRowContext(ctx, claimNextWorkflowSQL, params...)
	if err := row.Scan(&id, &defBytes, &priority, &createdAt, &attempts, &claimedBy, &claimedAt, &leaseVal, &metaBytes); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNoPending
		}
		return nil, err
	}

	var def workflow.Definition
	if err := json.Unmarshal(defBytes, &def); err != nil {
		_ = q.queries.ReleaseWorkflow(ctx, id)
		return nil, err
	}
	meta := map[string]any{}
	if len(metaBytes) > 0 {
		_ = json.Unmarshal(metaBytes, &meta)
	}

	claimed := &ClaimedWorkflow{
		QueuedWorkflow: QueuedWorkflow{
			ID:         WorkflowID(id),
			Definition: def,
			Priority:   Priority(priority),
			CreatedAt:  createdAt,
			Attempt:    int(attempts),
			Metadata:   meta,
		},
		LeaseID:  leaseID,
		WorkerID: workerID,
	}
	if claimedAt.Valid {
		claimed.ClaimedAt = claimedAt.Time
	}
	if leaseVal.Valid {
		claimed.LeaseID = leaseVal.String
	}
	if claimedBy.Valid {
		claimed.WorkerID = claimedBy.String
	}
	return claimed, nil
}

func (q *SQLiteQueue) Release(ctx context.Context, id WorkflowID) error {
	return q.queries.ReleaseWorkflow(ctx, string(id))
}

func (q *SQLiteQueue) Ack(ctx context.Context, id WorkflowID, summary ResultSummary) error {
	return q.queries.AckWorkflow(ctx, string(id))
}

func (q *SQLiteQueue) Cancel(ctx context.Context, id WorkflowID) error {
	return q.queries.CancelWorkflow(ctx, string(id))
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
	params := []any{
		sql.Named("required_tags", string(encodeStringSlice(sel.All))),
		sql.Named("any_tags", string(encodeStringSlice(sel.Any))),
		sql.Named("none_tags", string(encodeStringSlice(sel.None))),
	}
	row := q.db.QueryRowContext(ctx, pendingCountSQL, params...)
	var count int
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
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

func encodeStringSlice(values []string) []byte {
	if len(values) == 0 {
		return []byte("[]")
	}
	data, err := json.Marshal(values)
	if err != nil {
		return []byte("[]")
	}
	return data
}
