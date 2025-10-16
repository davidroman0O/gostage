package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3/state/sqlc"
)

func TestSQLiteStateReader_ListWorkflowsFilters(t *testing.T) {
	db := openTestDB(t)
	reader, err := NewSQLiteStateReader(db)
	if err != nil {
		t.Fatalf("NewSQLiteStateReader: %v", err)
	}
	queries := sqlc.New(db)
	now := time.Now().Add(-3 * time.Hour)

	insertWorkflow := func(t *testing.T, id, workflowType string, state WorkflowState, tags []string, createdAt time.Time) {
		t.Helper()
		tagJSON, err := json.Marshal(tags)
		if err != nil {
			t.Fatalf("marshal tags: %v", err)
		}
		if err := queries.UpsertWorkflowRun(context.Background(), sqlc.UpsertWorkflowRunParams{
			ID:          id,
			Name:        sqlNullString(id),
			Description: sqlNullString("desc-" + id),
			Type:        sqlNullString(workflowType),
			Tags:        tagJSON,
			Metadata:    []byte(`{}`),
			Column7:     createdAt,
			State:       string(state),
			Success:     boolToInt64(state == WorkflowCompleted),
			Error:       sqlNullString(""),
		}); err != nil {
			t.Fatalf("insert workflow %s: %v", id, err)
		}
	}

	insertWorkflow(t, "wf-1", "batch", WorkflowCompleted, []string{"payments", "us"}, now.Add(1*time.Hour))
	insertWorkflow(t, "wf-2", "batch", WorkflowFailed, []string{"payments", "eu"}, now.Add(2*time.Hour))
	insertWorkflow(t, "wf-3", "realtime", WorkflowCompleted, []string{"notifications"}, now.Add(3*time.Hour))

	tests := []struct {
		name   string
		filter StateFilter
		want   []string
	}{
		{
			name: "no filters",
			filter: StateFilter{
				Limit:  0,
				Offset: 0,
			},
			want: []string{"wf-3", "wf-2", "wf-1"},
		},
		{
			name: "tag filter",
			filter: StateFilter{
				Tags: []string{"payments"},
			},
			want: []string{"wf-2", "wf-1"},
		},
		{
			name: "state filter",
			filter: StateFilter{
				States: []WorkflowState{WorkflowCompleted},
			},
			want: []string{"wf-3", "wf-1"},
		},
		{
			name: "type filter",
			filter: StateFilter{
				Type: []string{"batch"},
			},
			want: []string{"wf-2", "wf-1"},
		},
		{
			name: "combined filter",
			filter: StateFilter{
				Tags:   []string{"payments"},
				States: []WorkflowState{WorkflowFailed},
			},
			want: []string{"wf-2"},
		},
		{
			name: "limit and offset",
			filter: StateFilter{
				Limit:  1,
				Offset: 1,
			},
			want: []string{"wf-2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := reader.ListWorkflows(context.Background(), tt.filter)
			if err != nil {
				t.Fatalf("ListWorkflows: %v", err)
			}
			ids := make([]string, len(results))
			for i, summary := range results {
				ids[i] = string(summary.ID)
			}
			if diff := compareStringSlices(ids, tt.want); diff != "" {
				t.Fatalf("unexpected workflow IDs:\n%s", diff)
			}
		})
	}
}

func sqlNullString(val string) sql.NullString {
	if val == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: val, Valid: true}
}

func compareStringSlices(got, want []string) string {
	if len(got) != len(want) {
		return formatDiff(got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			return formatDiff(got, want)
		}
	}
	return ""
}

func formatDiff(got, want []string) string {
	return "got " + stringifySlice(got) + ", want " + stringifySlice(want)
}

func stringifySlice(values []string) string {
	if len(values) == 0 {
		return "[]"
	}
	b, _ := json.Marshal(values)
	return string(b)
}
