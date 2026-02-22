package gostage

import (
	"context"
	"testing"
)

func TestCtx_GetSetGetOr(t *testing.T) {
	s := newRunState("test", nil)
	ctx := newCtx(context.Background(), s, NewDefaultLogger())

	Set(ctx, "name", "Alice")
	name := Get[string](ctx, "name")
	if name != "Alice" {
		t.Fatalf("expected Alice, got %s", name)
	}

	missing := Get[string](ctx, "nonexistent")
	if missing != "" {
		t.Fatalf("expected empty string, got %s", missing)
	}

	def := GetOr[string](ctx, "nonexistent", "default")
	if def != "default" {
		t.Fatalf("expected 'default', got %s", def)
	}
}

func TestCtx_Bail(t *testing.T) {
	s := newRunState("test", nil)
	ctx := newCtx(context.Background(), s, NewDefaultLogger())

	err := Bail(ctx, "too young")
	if err == nil {
		t.Fatal("expected error")
	}
	bailErr, ok := err.(*BailError)
	if !ok {
		t.Fatalf("expected BailError, got %T", err)
	}
	if bailErr.Reason != "too young" {
		t.Fatalf("expected 'too young', got %s", bailErr.Reason)
	}
}

func TestCtx_Suspend(t *testing.T) {
	s := newRunState("test", nil)
	ctx := newCtx(context.Background(), s, NewDefaultLogger())

	err := Suspend(ctx, P{"reason": "approval"})
	if err == nil {
		t.Fatal("expected error")
	}
	suspendErr, ok := err.(*SuspendError)
	if !ok {
		t.Fatalf("expected SuspendError, got %T", err)
	}
	if suspendErr.Data["reason"] != "approval" {
		t.Fatalf("expected reason 'approval', got %v", suspendErr.Data["reason"])
	}
}

func TestCtx_IsResuming(t *testing.T) {
	s := newRunState("test", nil)
	ctx := newCtx(context.Background(), s, NewDefaultLogger())

	if IsResuming(ctx) {
		t.Fatal("expected not resuming")
	}

	ctx.resuming = true
	if !IsResuming(ctx) {
		t.Fatal("expected resuming")
	}
}

func TestCtx_ResumeData(t *testing.T) {
	s := newRunState("test", nil)
	s.Set("__resume:approved", true)
	ctx := newCtx(context.Background(), s, NewDefaultLogger())

	approved := ResumeData[bool](ctx, "approved")
	if !approved {
		t.Fatal("expected approved to be true")
	}
}

func TestCtx_ItemAndItemIndex(t *testing.T) {
	s := newRunState("test", nil)
	ctx := newCtx(context.Background(), s, NewDefaultLogger())
	ctx.forEachItem = "hello"
	ctx.forEachIndex = 7

	item := Item[string](ctx)
	if item != "hello" {
		t.Fatalf("expected 'hello', got %s", item)
	}

	idx := ItemIndex(ctx)
	if idx != 7 {
		t.Fatalf("expected 7, got %d", idx)
	}
}

func TestCtx_Item_NilReturnsZero(t *testing.T) {
	s := newRunState("test", nil)
	ctx := newCtx(context.Background(), s, NewDefaultLogger())

	item := Item[string](ctx)
	if item != "" {
		t.Fatalf("expected empty string, got %s", item)
	}
}

func TestCtx_Send_NoOp(t *testing.T) {
	s := newRunState("test", nil)
	ctx := newCtx(context.Background(), s, NewDefaultLogger())

	// No sendFn configured — should be no-op
	err := Send(ctx, "progress", P{"pct": 50})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCtx_Send_WithHandler(t *testing.T) {
	s := newRunState("test", nil)
	ctx := newCtx(context.Background(), s, NewDefaultLogger())

	var received string
	ctx.sendFn = func(msgType string, payload any) error {
		received = msgType
		return nil
	}

	err := Send(ctx, "progress", nil)
	if err != nil {
		t.Fatal(err)
	}
	if received != "progress" {
		t.Fatalf("expected 'progress', got %s", received)
	}
}

func TestCtx_Context(t *testing.T) {
	s := newRunState("test", nil)
	goCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx := newCtx(goCtx, s, NewDefaultLogger())

	if ctx.Context() != goCtx {
		t.Fatal("expected same context")
	}
}
