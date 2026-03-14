package gostage

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestE2E_ETLPipeline(t *testing.T) {
	ResetTaskRegistry()

	Task("etl.extract_users", func(ctx *Ctx) error {
		Set(ctx, "users", []string{"alice", "bob", "charlie"})
		return nil
	})
	Task("etl.extract_orders", func(ctx *Ctx) error {
		Set(ctx, "orders", []string{"ORD-1", "ORD-2"})
		return nil
	})
	Task("etl.extract_products", func(ctx *Ctx) error {
		Set(ctx, "products", []string{"widget", "gadget", "gizmo", "doohickey"})
		return nil
	})
	Task("etl.transform", func(ctx *Ctx) error {
		users := Get[[]any](ctx, "users")
		orders := Get[[]any](ctx, "orders")
		products := Get[[]any](ctx, "products")
		Set(ctx, "total_records", len(users)+len(orders)+len(products))
		Set(ctx, "transformed", true)
		return nil
	})
	Task("etl.load", func(ctx *Ctx) error {
		total := Get[int](ctx, "total_records")
		if total == 0 {
			return fmt.Errorf("no records to load")
		}
		Set(ctx, "loaded", true)
		Set(ctx, "load_count", total)
		return nil
	})

	wf, err := NewWorkflow("etl-pipeline").
		Parallel(
			Step("etl.extract_users"),
			Step("etl.extract_orders"),
			Step("etl.extract_products"),
		).
		Step("etl.transform").
		Step("etl.load").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}

	loadCount, _ := ResultGet[int](result, "load_count")
	if loadCount != 9 {
		t.Fatalf("expected 9 total records, got %d", loadCount)
	}
}

func TestE2E_PipelineWithRetry(t *testing.T) {
	ResetTaskRegistry()

	var attempts atomic.Int32
	Task("rp.flaky_extract", func(ctx *Ctx) error {
		n := attempts.Add(1)
		if n < 3 {
			return fmt.Errorf("transient error attempt %d", n)
		}
		Set(ctx, "data", "extracted")
		return nil
	}, WithRetry(5), WithRetryStrategy(ExponentialBackoff(5*time.Millisecond, 100*time.Millisecond)))

	Task("rp.process", func(ctx *Ctx) error {
		data := Get[string](ctx, "data")
		if data != "extracted" {
			return fmt.Errorf("expected extracted data")
		}
		Set(ctx, "processed", true)
		return nil
	})

	wf, err := NewWorkflow("retry-pipeline").
		Step("rp.flaky_extract").
		Step("rp.process").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed with retry, got %s", result.Status)
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts.Load())
	}
}
