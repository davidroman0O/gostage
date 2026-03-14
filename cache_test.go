package gostage

import (
	"context"
	"testing"
)

// === Cache Size ===

func TestWithCacheSize(t *testing.T) {
	ResetTaskRegistry()

	Task("cache.t1", func(ctx *Ctx) error { return nil })
	Task("cache.t2", func(ctx *Ctx) error { return nil })
	Task("cache.t3", func(ctx *Ctx) error { return nil })

	wf1, err := NewWorkflow("cache-wf-1").Step("cache.t1").Commit()
	if err != nil {
		t.Fatal(err)
	}
	wf2, err := NewWorkflow("cache-wf-2").Step("cache.t2").Commit()
	if err != nil {
		t.Fatal(err)
	}
	wf3, err := NewWorkflow("cache-wf-3").Step("cache.t3").Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New(WithCacheSize(2))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	engine.RunSync(context.Background(), wf1, nil)
	engine.RunSync(context.Background(), wf2, nil)
	engine.RunSync(context.Background(), wf3, nil)

	// First workflow should have been evicted
	engine.workflowCacheMu.RLock()
	cacheLen := len(engine.workflowCache)
	_, hasWf1 := engine.workflowCache["cache-wf-1"]
	_, hasWf2 := engine.workflowCache["cache-wf-2"]
	_, hasWf3 := engine.workflowCache["cache-wf-3"]
	engine.workflowCacheMu.RUnlock()

	if cacheLen != 2 {
		t.Fatalf("expected cache size 2, got %d", cacheLen)
	}
	if hasWf1 {
		t.Fatal("expected cache-wf-1 to be evicted")
	}
	if !hasWf2 || !hasWf3 {
		t.Fatal("expected cache-wf-2 and cache-wf-3 to be cached")
	}
}

// === Decision 008: Test Coverage Gaps ===

func TestCacheLRUEviction(t *testing.T) {
	ResetTaskRegistry()

	Task("d8.lru_noop", func(ctx *Ctx) error { return nil })

	wf1, _ := NewWorkflow("lru-wf-1").Step("d8.lru_noop").Commit()
	wf2, _ := NewWorkflow("lru-wf-2").Step("d8.lru_noop").Commit()
	wf3, _ := NewWorkflow("lru-wf-3").Step("d8.lru_noop").Commit()

	engine, err := New(WithCacheSize(2))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()

	// Run wf1, wf2 -> cache: [wf1, wf2]
	if _, err := engine.RunSync(ctx, wf1, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.RunSync(ctx, wf2, nil); err != nil {
		t.Fatal(err)
	}

	// Run wf1 again -> promotes wf1, cache: [wf2, wf1]
	if _, err := engine.RunSync(ctx, wf1, nil); err != nil {
		t.Fatal(err)
	}

	// Run wf3 -> evicts wf2 (LRU), cache: [wf1, wf3]
	if _, err := engine.RunSync(ctx, wf3, nil); err != nil {
		t.Fatal(err)
	}

	// wf1 should still be cached (was promoted)
	if engine.lookupCachedWorkflow("lru-wf-1") == nil {
		t.Fatal("wf1 should still be in cache after LRU promotion")
	}
	// wf2 should be evicted (LRU victim)
	if engine.lookupCachedWorkflow("lru-wf-2") != nil {
		t.Fatal("wf2 should have been evicted as LRU")
	}
	// wf3 should be cached
	if engine.lookupCachedWorkflow("lru-wf-3") == nil {
		t.Fatal("wf3 should be in cache")
	}
}

// === LRU Cache Promotion Order (Issue 8) ===

func TestCacheLRU_PromotionOrder(t *testing.T) {
	ResetTaskRegistry()

	Task("lru.noop", func(ctx *Ctx) error { return nil })

	wfA, _ := NewWorkflow("lru-promo-a").Step("lru.noop").Commit()
	wfB, _ := NewWorkflow("lru-promo-b").Step("lru.noop").Commit()
	wfC, _ := NewWorkflow("lru-promo-c").Step("lru.noop").Commit()
	wfD, _ := NewWorkflow("lru-promo-d").Step("lru.noop").Commit()

	// Cache size = 2
	engine, err := New(WithCacheSize(2))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()

	// Run A, B -> cache: [A, B]
	engine.RunSync(ctx, wfA, nil)
	engine.RunSync(ctx, wfB, nil)

	// Promote A by running it again -> cache: [B, A] where A is MRU
	engine.RunSync(ctx, wfA, nil)

	// Run C -> evicts LRU which is B (not A), cache: [A, C]
	engine.RunSync(ctx, wfC, nil)

	engine.workflowCacheMu.RLock()
	hasA := engine.workflowCache["lru-promo-a"] != nil
	hasB := engine.workflowCache["lru-promo-b"] != nil
	hasC := engine.workflowCache["lru-promo-c"] != nil
	engine.workflowCacheMu.RUnlock()

	if !hasA {
		t.Fatal("A should still be cached (was promoted before C evicted B)")
	}
	if hasB {
		t.Fatal("B should have been evicted as LRU")
	}
	if !hasC {
		t.Fatal("C should be in cache")
	}

	// Run D -> evicts LRU which is A, cache: [C, D]
	engine.RunSync(ctx, wfD, nil)

	engine.workflowCacheMu.RLock()
	hasA = engine.workflowCache["lru-promo-a"] != nil
	engine.workflowCacheMu.RUnlock()

	if hasA {
		t.Fatal("A should now be evicted (became LRU after C was used)")
	}
}
