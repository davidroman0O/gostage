package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

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
	if err := q.queries.EnqueueWorkflow(ctx, sqlc.EnqueueWorkflowParams{
		ID:         string(id),
		Definition: payload,
		Priority:   int64(priority),
		Metadata:   metaBytes,
	}); err != nil {
		return "", err
	}
	return id, nil
}

func (q *SQLiteQueue) Claim(ctx context.Context, sel Selector, workerID string) (*ClaimedWorkflow, error) {
	leaseID := uuid.NewString()
	worker := sql.NullString{String: workerID, Valid: workerID != ""}
	lease := sql.NullString{String: leaseID, Valid: true}
	for attempts := 0; attempts < 10; attempts++ {
		row, err := q.queries.ClaimNextWorkflow(ctx, sqlc.ClaimNextWorkflowParams{
			ClaimedBy: worker,
			LeaseID:   lease,
		})
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNoPending
		}
		if err != nil {
			return nil, err
		}
		var def workflow.Definition
		if err := json.Unmarshal(row.Definition, &def); err != nil {
			_ = q.queries.ReleaseWorkflow(ctx, row.ID)
			return nil, err
		}
		if matchesSelector(def.Tags, sel) {
			meta := map[string]any{}
			if len(row.Metadata) > 0 {
				_ = json.Unmarshal(row.Metadata, &meta)
			}
			claimed := &ClaimedWorkflow{
				QueuedWorkflow: QueuedWorkflow{
					ID:         WorkflowID(row.ID),
					Definition: def,
					Priority:   Priority(row.Priority),
					CreatedAt:  row.CreatedAt,
					Attempt:    int(row.Attempts),
					Metadata:   meta,
				},
				LeaseID:  leaseID,
				WorkerID: workerID,
			}
			if row.ClaimedAt.Valid {
				claimed.ClaimedAt = row.ClaimedAt.Time
			}
			if row.LeaseID.Valid {
				claimed.LeaseID = row.LeaseID.String
			}
			return claimed, nil
		}
		// Not a selector match; release and try again.
		_ = q.queries.ReleaseWorkflow(ctx, row.ID)
	}
	return nil, ErrNoPending
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
