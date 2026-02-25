package gostage

import (
	"context"
	"path/filepath"
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

	err := Suspend(ctx, Params{"reason": "approval"})
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
	err := Send(ctx, "progress", Params{"pct": 50})
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

// === Set[T] Validation ===

func TestSetRejectsNonSerializable(t *testing.T) {
	s := newRunState("test", nil)
	ctx := newCtx(context.Background(), s, NewDefaultLogger())

	// chan
	if err := Set(ctx, "ch", make(chan int)); err == nil {
		t.Fatal("expected error for chan type")
	}

	// func
	if err := Set(ctx, "fn", func() {}); err == nil {
		t.Fatal("expected error for func type")
	}

	// complex
	if err := Set(ctx, "cx", complex(1, 2)); err == nil {
		t.Fatal("expected error for complex type")
	}

	// struct with chan field
	type BadStruct struct {
		Ch chan int
	}
	if err := Set(ctx, "bad", BadStruct{}); err == nil {
		t.Fatal("expected error for struct with chan field")
	}
}

func TestSetAcceptsSerializableStruct(t *testing.T) {
	s := newRunState("test", nil)
	ctx := newCtx(context.Background(), s, NewDefaultLogger())

	type User struct {
		Name  string `json:"name"`
		Age   int    `json:"age"`
		Email string `json:"email"`
	}
	err := Set(ctx, "user", User{Name: "Alice", Age: 30, Email: "alice@example.com"})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestSetAcceptsPrimitives(t *testing.T) {
	s := newRunState("test", nil)
	ctx := newCtx(context.Background(), s, NewDefaultLogger())

	if err := Set(ctx, "i", 42); err != nil {
		t.Fatalf("int: %v", err)
	}
	if err := Set(ctx, "s", "hello"); err != nil {
		t.Fatalf("string: %v", err)
	}
	if err := Set(ctx, "f", 3.14); err != nil {
		t.Fatalf("float64: %v", err)
	}
	if err := Set(ctx, "b", true); err != nil {
		t.Fatalf("bool: %v", err)
	}
}

// === Struct Round-Trip via coerce[T] ===

func TestStructCoercion(t *testing.T) {
	type Config struct {
		Host string `json:"host"`
		Port int    `json:"port"`
	}

	s := newRunState("test", nil)
	ctx := newCtx(context.Background(), s, NewDefaultLogger())

	// Store struct normally
	if err := Set(ctx, "cfg", Config{Host: "localhost", Port: 8080}); err != nil {
		t.Fatal(err)
	}

	// Direct retrieval works (type assertion path)
	cfg := Get[Config](ctx, "cfg")
	if cfg.Host != "localhost" || cfg.Port != 8080 {
		t.Fatalf("direct Get failed: %+v", cfg)
	}

	// Simulate JSON round-trip: store as map[string]interface{} (what JSON unmarshal into any produces)
	s.Set("cfg_json", map[string]interface{}{
		"host": "example.com",
		"port": float64(443), // JSON numbers become float64
	})

	// coerce[T] should re-marshal back to Config
	cfg2 := Get[Config](ctx, "cfg_json")
	if cfg2.Host != "example.com" {
		t.Fatalf("expected host 'example.com', got %q", cfg2.Host)
	}
	if cfg2.Port != 443 {
		t.Fatalf("expected port 443, got %d", cfg2.Port)
	}
}

func TestSliceOfStructsCoercion(t *testing.T) {
	type Item struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}

	s := newRunState("test", nil)
	ctx := newCtx(context.Background(), s, NewDefaultLogger())

	// Simulate JSON round-trip: []interface{} with map entries
	s.Set("items", []interface{}{
		map[string]interface{}{"name": "a", "value": float64(1)},
		map[string]interface{}{"name": "b", "value": float64(2)},
	})

	items := Get[[]Item](ctx, "items")
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
	if items[0].Name != "a" || items[0].Value != 1 {
		t.Fatalf("item 0: %+v", items[0])
	}
	if items[1].Name != "b" || items[1].Value != 2 {
		t.Fatalf("item 1: %+v", items[1])
	}
}

func TestMapOfStructsCoercion(t *testing.T) {
	type Score struct {
		Points int  `json:"points"`
		Passed bool `json:"passed"`
	}

	s := newRunState("test", nil)
	ctx := newCtx(context.Background(), s, NewDefaultLogger())

	// Simulate JSON round-trip
	s.Set("scores", map[string]interface{}{
		"alice": map[string]interface{}{"points": float64(95), "passed": true},
		"bob":   map[string]interface{}{"points": float64(42), "passed": false},
	})

	scores := Get[map[string]Score](ctx, "scores")
	if len(scores) != 2 {
		t.Fatalf("expected 2 scores, got %d", len(scores))
	}
	if scores["alice"].Points != 95 || !scores["alice"].Passed {
		t.Fatalf("alice: %+v", scores["alice"])
	}
	if scores["bob"].Points != 42 || scores["bob"].Passed {
		t.Fatalf("bob: %+v", scores["bob"])
	}
}

func TestStructPointerCoercion(t *testing.T) {
	type Settings struct {
		Debug bool `json:"debug"`
	}

	s := newRunState("test", nil)
	ctx := newCtx(context.Background(), s, NewDefaultLogger())

	// Simulate JSON round-trip
	s.Set("settings", map[string]interface{}{"debug": true})

	result := Get[*Settings](ctx, "settings")
	if result == nil {
		t.Fatal("expected non-nil pointer")
	}
	if !result.Debug {
		t.Fatal("expected debug=true")
	}
}

// === Struct Persistence Round-Trip ===

func TestStructSurvivesPersistenceRoundTrip(t *testing.T) {
	type User struct {
		Name  string `json:"name"`
		Age   int    `json:"age"`
		Admin bool   `json:"admin"`
	}

	dir := t.TempDir()
	p, err := newSQLitePersistence(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	bgCtx := context.Background()

	// Write struct to state and flush
	s := newRunState("run-struct", p)
	s.Set("user", User{Name: "Alice", Age: 30, Admin: true})
	if err := s.Flush(bgCtx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Load into fresh state (simulates restart)
	s2 := newRunState("run-struct", p)
	if err := s2.LoadFromPersistence(bgCtx); err != nil {
		t.Fatalf("Load: %v", err)
	}

	// Raw Get returns map[string]interface{} after JSON unmarshal
	raw, ok := s2.Get("user")
	if !ok {
		t.Fatal("expected user key to exist")
	}
	if _, isMap := raw.(map[string]interface{}); !isMap {
		t.Fatalf("expected map[string]interface{} after load, got %T", raw)
	}

	// coerce[T] should restore the struct type
	ctx := newCtx(bgCtx, s2, NewDefaultLogger())
	user := Get[User](ctx, "user")
	if user.Name != "Alice" {
		t.Fatalf("expected Name=Alice, got %q", user.Name)
	}
	if user.Age != 30 {
		t.Fatalf("expected Age=30, got %d", user.Age)
	}
	if !user.Admin {
		t.Fatal("expected Admin=true")
	}
}

// === Struct Spawn Round-Trip ===

func TestStructSurvivesSpawnRoundTrip(t *testing.T) {
	type Task struct {
		ID       string `json:"id"`
		Priority int    `json:"priority"`
	}

	s := newRunState("test-spawn-struct", nil)
	s.Set("task", Task{ID: "abc", Priority: 5})

	// Serialize (parent → child)
	data, err := serializeStateForChild(s, nil, 0)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	// Deserialize (child receives)
	result, err := deserializeStoreData(data)
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}

	// After deserialize, the value is map[string]interface{} (JSON behavior)
	// Build a Ctx and use Get[T] to coerce it back
	s2 := newRunState("test-spawn-struct-child", nil)
	for k, v := range result {
		s2.SetClean(k, v)
	}
	ctx := newCtx(context.Background(), s2, NewDefaultLogger())

	task := Get[Task](ctx, "task")
	if task.ID != "abc" {
		t.Fatalf("expected ID=abc, got %q", task.ID)
	}
	if task.Priority != 5 {
		t.Fatalf("expected Priority=5, got %d", task.Priority)
	}
}

func TestSliceOfStructsSurvivesSpawnRoundTrip(t *testing.T) {
	type Record struct {
		Key   string `json:"key"`
		Value int    `json:"value"`
	}

	s := newRunState("test-spawn-slice", nil)
	s.Set("records", []Record{
		{Key: "a", Value: 1},
		{Key: "b", Value: 2},
	})

	data, err := serializeStateForChild(s, nil, 0)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	result, err := deserializeStoreData(data)
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}

	s2 := newRunState("child", nil)
	for k, v := range result {
		s2.SetClean(k, v)
	}
	ctx := newCtx(context.Background(), s2, NewDefaultLogger())

	records := Get[[]Record](ctx, "records")
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	if records[0].Key != "a" || records[0].Value != 1 {
		t.Fatalf("record 0: %+v", records[0])
	}
	if records[1].Key != "b" || records[1].Value != 2 {
		t.Fatalf("record 1: %+v", records[1])
	}
}
