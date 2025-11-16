package gostage_test

import (
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
	if summary.ID != "" {
		t.Fatalf("expected zero summary ID")
	}

	state := gostage.WorkflowPending
	if state != gostage.WorkflowPending {
		t.Fatalf("unexpected workflow state")
	}

	// Ensure helper functions accept the alias types without importing state directly.
	node := &gostage.Node{}
	_ = node.StatsWithContext
}
