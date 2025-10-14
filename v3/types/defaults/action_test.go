package defaults

import (
	"testing"

	"github.com/davidroman0O/gostage/v3/types"
)

func TestBuildActionSuccess(t *testing.T) {
	const actionID = "build-action-success"

	MustRegisterAction(actionID, "test action", func(types.Context) error { return nil }, WithActionTags("foo"))

	act, err := BuildAction(actionID)
	if err != nil {
		t.Fatalf("BuildAction returned error: %v", err)
	}

	if act.Name() != actionID {
		t.Fatalf("expected action name %q, got %q", actionID, act.Name())
	}

	tags := act.Tags()
	if len(tags) != 1 || tags[0] != "foo" {
		t.Fatalf("expected tags [foo], got %v", tags)
	}
}

func TestBuildActionMissing(t *testing.T) {
	if _, err := BuildAction("missing-action-id"); err == nil {
		t.Fatalf("expected error when building missing action")
	}
}
