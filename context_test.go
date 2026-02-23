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

func TestCtx_NumericCoercion(t *testing.T) {
	s := newRunState("test", nil)
	ctx := newCtx(context.Background(), s, NewDefaultLogger())

	// int → int: direct assertion (no coercion needed)
	Set(ctx, "count", 42)
	if v := Get[int](ctx, "count"); v != 42 {
		t.Fatalf("Get[int] expected 42, got %d", v)
	}

	// float64 → float64: direct assertion
	Set(ctx, "ratio", 3.14)
	if v := Get[float64](ctx, "ratio"); v != 3.14 {
		t.Fatalf("Get[float64] expected 3.14, got %f", v)
	}

	// int → float64: coercion (int stored, read as float64)
	if v := Get[float64](ctx, "count"); v != 42.0 {
		t.Fatalf("Get[float64] from int expected 42.0, got %f", v)
	}

	// float64 → int: coercion with no fractional loss
	Set(ctx, "whole", 3.0)
	if v := Get[int](ctx, "whole"); v != 3 {
		t.Fatalf("Get[int] from 3.0 expected 3, got %d", v)
	}

	// float64 → int: coercion blocked by fractional part
	if v := Get[int](ctx, "ratio"); v != 0 {
		t.Fatalf("Get[int] from 3.14 expected 0 (blocked), got %d", v)
	}

	// GetOr with coercion
	if v := GetOr[float64](ctx, "count", 0.0); v != 42.0 {
		t.Fatalf("GetOr[float64] from int expected 42.0, got %f", v)
	}

	// GetOr fallback when key missing
	if v := GetOr[int](ctx, "missing", 99); v != 99 {
		t.Fatalf("GetOr[int] missing expected 99, got %d", v)
	}

	// Simulate JSON round-trip: float64 stored (as JSON would), read as int
	s.Set("json_num", float64(100))
	if v := Get[int](ctx, "json_num"); v != 100 {
		t.Fatalf("Get[int] from float64(100) expected 100, got %d", v)
	}
}

func TestCtx_ItemCoercion(t *testing.T) {
	s := newRunState("test", nil)
	ctx := newCtx(context.Background(), s, NewDefaultLogger())

	// Simulate JSON-decoded ForEach item (float64 instead of int)
	ctx.forEachItem = float64(7)
	ctx.forEachIndex = 0

	if v := Item[int](ctx); v != 7 {
		t.Fatalf("Item[int] from float64(7) expected 7, got %d", v)
	}
	if v := Item[float64](ctx); v != 7.0 {
		t.Fatalf("Item[float64] expected 7.0, got %f", v)
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
