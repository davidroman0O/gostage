package gostage

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestE2E_OrderProcessing(t *testing.T) {
	ResetTaskRegistry()

	Task("order.validate", func(ctx *Ctx) error {
		orderID := Get[string](ctx, "order_id")
		if orderID == "" {
			return errors.New("order_id is required")
		}
		Set(ctx, "validated", true)
		Set(ctx, "validation_time", time.Now().Format(time.RFC3339))
		return nil
	})

	Task("order.charge", func(ctx *Ctx) error {
		if IsResuming(ctx) {
			approved := Get[bool](ctx, "__resume:approved")
			if !approved {
				return errors.New("payment not approved")
			}
			Set(ctx, "charged", true)
			return nil
		}
		return Suspend(ctx, Params{
			"reason":   "awaiting_payment",
			"amount":   Get[float64](ctx, "amount"),
			"order_id": Get[string](ctx, "order_id"),
		})
	})

	Task("order.ship", func(ctx *Ctx) error {
		if !Get[bool](ctx, "charged") {
			return errors.New("cannot ship uncharged order")
		}
		Set(ctx, "shipped", true)
		Set(ctx, "tracking_number", "TRK-12345")
		return nil
	})

	wf, err := NewWorkflow("order-process").
		Step("order.validate").
		Step("order.charge").
		Step("order.ship").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	dbPath := filepath.Join(t.TempDir(), "order.db")
	engine, err := New(WithSQLite(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// Phase 1: Start order — should suspend at charge step
	result, err := engine.RunSync(context.Background(), wf, Params{
		"order_id": "ORD-001",
		"amount":   99.99,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Suspended {
		t.Fatalf("expected Suspended, got %s", result.Status)
	}
	if result.SuspendData["reason"] != "awaiting_payment" {
		t.Fatalf("expected reason 'awaiting_payment', got %v", result.SuspendData["reason"])
	}

	validated, _ := ResultGet[bool](result, "validated")
	if !validated {
		t.Fatal("expected validated=true at suspend point")
	}

	// Phase 2: Resume with approval
	result2, err := engine.Resume(context.Background(), result.RunID, Params{"approved": true})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if result2.Status != Completed {
		t.Fatalf("expected Completed after resume, got %s", result2.Status)
	}

	charged, _ := ResultGet[bool](result2, "charged")
	shipped, _ := ResultGet[bool](result2, "shipped")
	tracking, _ := ResultGet[string](result2, "tracking_number")
	if !charged {
		t.Fatal("expected charged=true")
	}
	if !shipped {
		t.Fatal("expected shipped=true")
	}
	if tracking != "TRK-12345" {
		t.Fatalf("expected tracking TRK-12345, got %s", tracking)
	}
}

func TestE2E_OrderProcessing_PaymentRejected(t *testing.T) {
	ResetTaskRegistry()

	Task("rej.validate", func(ctx *Ctx) error {
		Set(ctx, "validated", true)
		return nil
	})
	Task("rej.charge", func(ctx *Ctx) error {
		if IsResuming(ctx) {
			approved := Get[bool](ctx, "__resume:approved")
			if !approved {
				return errors.New("payment rejected by processor")
			}
			return nil
		}
		return Suspend(ctx, Params{"reason": "awaiting_payment"})
	})
	Task("rej.ship", func(ctx *Ctx) error {
		return nil
	})

	wf, err := NewWorkflow("rej-order").
		Step("rej.validate").
		Step("rej.charge").
		Step("rej.ship").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	dbPath := filepath.Join(t.TempDir(), "rej.db")
	engine, err := New(WithSQLite(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Status != Suspended {
		t.Fatalf("expected Suspended, got %s", result.Status)
	}

	result2, err := engine.Resume(context.Background(), result.RunID, Params{"approved": false})
	if err != nil {
		t.Fatal(err)
	}
	if result2.Status != Failed {
		t.Fatalf("expected Failed after rejection, got %s", result2.Status)
	}
}
