package gostage_test

import (
	"context"
	"testing"

	gostage "github.com/davidroman0O/gostage/v3"
)

func TestStateAliasesExposePublicAPI(t *testing.T) {
	var reader gostage.StateReader
	_ = reader

	var filter gostage.StateFilter
	filter.Tags = []string{"example"}
	_ = filter

	var summary gostage.WorkflowSummary
	if summary.WorkflowRecord.ID != "" {
		t.Fatalf("expected zero summary ID")
	}

	state := gostage.WorkflowPending
	if state != gostage.WorkflowPending {
		t.Fatalf("unexpected workflow state")
	}

	// Ensure helper functions accept the alias types without importing state directly.
	node := &gostage.Node{}
	ctx := context.Background()
	_, _ = node.StatsWithContext(ctx)
}
