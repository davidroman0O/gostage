package gostage_test

import (
	"context"
	"testing"

	gostage "github.com/davidroman0O/gostage/v3"
)

func TestRunStub(t *testing.T) {
	t.Parallel()

	node, diag, err := gostage.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node == nil {
		t.Fatalf("expected node handle")
	}
	if diag == nil {
		t.Fatalf("diagnostic channel must be returned")
	}
	_ = node.Close()
}
