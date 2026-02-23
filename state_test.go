package gostage

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
)

func TestState_SetGetBasic(t *testing.T) {
	s := newRunState("test", nil)

	s.Set("name", "Alice")
	v, ok := s.Get("name")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if v != "Alice" {
		t.Fatalf("expected Alice, got %v", v)
	}

	_, ok = s.Get("nonexistent")
	if ok {
		t.Fatal("expected key not to exist")
	}
}

func TestState_DirtyTracking(t *testing.T) {
	s := newRunState("test", nil)

	// SetClean should not mark dirty
	s.SetClean("parent_key", "from_parent")
	dirty := s.ExportDirty()
	if len(dirty) != 0 {
		t.Fatalf("expected 0 dirty keys, got %d", len(dirty))
	}

	// Set should mark dirty
	s.Set("child_key", "from_child")
	dirty = s.ExportDirty()
	if len(dirty) != 1 {
		t.Fatalf("expected 1 dirty key, got %d", len(dirty))
	}
	if dirty["child_key"] != "from_child" {
		t.Fatalf("expected 'from_child', got %v", dirty["child_key"])
	}

	// ExportAll should return all keys
	all := s.ExportAll()
	if len(all) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(all))
	}
}

func TestState_Delete(t *testing.T) {
	s := newRunState("test", nil)
	s.Set("key", "value")
	s.Delete("key")

	_, ok := s.Get("key")
	if ok {
		t.Fatal("expected key to be deleted")
	}
}

func TestState_Keys(t *testing.T) {
	s := newRunState("test", nil)
	s.Set("a", 1)
	s.Set("b", 2)
	s.SetClean("c", 3)

	keys := s.Keys()
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}
}

func TestState_Clone(t *testing.T) {
	s := newRunState("test", nil)
	s.Set("key", "value")

	c := s.Clone()

	// Clone should have the value
	v, ok := c.Get("key")
	if !ok || v != "value" {
		t.Fatalf("expected clone to have key, got %v, %v", v, ok)
	}

	// Clone entries should be clean
	dirty := c.ExportDirty()
	if len(dirty) != 0 {
		t.Fatalf("expected clone to have 0 dirty keys, got %d", len(dirty))
	}

	// Mutating clone should not affect original
	c.Set("key", "modified")
	orig, _ := s.Get("key")
	if orig != "value" {
		t.Fatalf("expected original to be unchanged, got %v", orig)
	}
}

func TestState_FlushAndLoad(t *testing.T) {
	dir := t.TempDir()
	p, err := newSQLitePersistence(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	ctx := context.Background()

	// Write some state and flush
	s := newRunState("run-flush", p)
	s.Set("name", "Alice")
	s.Set("count", 42)
	s.Set("active", true)

	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Load into a new runState
	s2 := newRunState("run-flush", p)
	if err := s2.LoadFromPersistence(ctx); err != nil {
		t.Fatalf("LoadFromPersistence failed: %v", err)
	}

	name, ok := s2.Get("name")
	if !ok || name != "Alice" {
		t.Fatalf("expected name=Alice, got %v", name)
	}

	active, ok := s2.Get("active")
	if !ok || active != true {
		t.Fatalf("expected active=true, got %v", active)
	}
}

func TestState_TypeFidelity(t *testing.T) {
	dir := t.TempDir()
	p, err := newSQLitePersistence(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	ctx := context.Background()

	s := newRunState("run-types", p)
	s.Set("i", int(42))
	s.Set("i64", int64(100))
	s.Set("f32", float32(3.14))
	s.Set("str", "hello")
	s.Set("b", true)

	if err := s.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Load into fresh state
	s2 := newRunState("run-types", p)
	if err := s2.LoadFromPersistence(ctx); err != nil {
		t.Fatalf("LoadFromPersistence failed: %v", err)
	}

	// int should stay int, not float64
	if v, ok := s2.Get("i"); !ok {
		t.Fatal("missing key 'i'")
	} else if _, isInt := v.(int); !isInt {
		t.Fatalf("expected int, got %T (%v)", v, v)
	} else if v.(int) != 42 {
		t.Fatalf("expected 42, got %v", v)
	}

	// int64 should stay int64
	if v, ok := s2.Get("i64"); !ok {
		t.Fatal("missing key 'i64'")
	} else if _, isI64 := v.(int64); !isI64 {
		t.Fatalf("expected int64, got %T (%v)", v, v)
	} else if v.(int64) != 100 {
		t.Fatalf("expected 100, got %v", v)
	}

	// float32 should stay float32
	if v, ok := s2.Get("f32"); !ok {
		t.Fatal("missing key 'f32'")
	} else if _, isF32 := v.(float32); !isF32 {
		t.Fatalf("expected float32, got %T (%v)", v, v)
	}

	// string should stay string
	if v, ok := s2.Get("str"); !ok {
		t.Fatal("missing key 'str'")
	} else if v != "hello" {
		t.Fatalf("expected 'hello', got %v", v)
	}

	// bool should stay bool
	if v, ok := s2.Get("b"); !ok {
		t.Fatal("missing key 'b'")
	} else if v != true {
		t.Fatalf("expected true, got %v", v)
	}
}

func TestState_ChildIsolation(t *testing.T) {
	s := newRunState("child", nil)

	// Parent data loaded via SetClean — not dirty
	s.SetClean("parent_name", "Alice")
	s.SetClean("parent_age", 30)

	// Child writes via Set — dirty
	s.Set("child_result", "processed")
	s.Set("child_score", 95)

	// ExportDirty should return only child writes
	dirty := s.ExportDirty()
	if len(dirty) != 2 {
		t.Fatalf("expected 2 dirty keys, got %d", len(dirty))
	}
	if dirty["child_result"] != "processed" {
		t.Fatalf("expected child_result='processed', got %v", dirty["child_result"])
	}
	if dirty["child_score"] != 95 {
		t.Fatalf("expected child_score=95, got %v", dirty["child_score"])
	}

	// Parent keys should NOT be in dirty
	if _, ok := dirty["parent_name"]; ok {
		t.Fatal("parent_name should not be in dirty export")
	}
	if _, ok := dirty["parent_age"]; ok {
		t.Fatal("parent_age should not be in dirty export")
	}

	// ExportAll should have everything
	all := s.ExportAll()
	if len(all) != 4 {
		t.Fatalf("expected 4 total keys, got %d", len(all))
	}
}

func TestState_FlushNilPersistence(t *testing.T) {
	s := newRunState("test", nil)
	s.Set("key", "value")

	// Flush with nil persistence should be no-op
	err := s.Flush(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestState_LoadNilPersistence(t *testing.T) {
	s := newRunState("test", nil)

	// Load with nil persistence should be no-op
	err := s.LoadFromPersistence(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

// === isJSONSerializable ===

func TestIsJSONSerializable_Primitives(t *testing.T) {
	cases := []struct {
		name string
		val  any
	}{
		{"int", int(0)},
		{"string", ""},
		{"float64", float64(0)},
		{"bool", false},
		{"uint32", uint32(0)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !isJSONSerializable(reflect.TypeOf(tc.val)) {
				t.Fatalf("expected %s to be serializable", tc.name)
			}
		})
	}
}

func TestIsJSONSerializable_Structs(t *testing.T) {
	type Good struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	type WithIgnored struct {
		Name string        `json:"name"`
		Ch   chan struct{} `json:"-"`
	}
	type Bad struct {
		Name string
		Ch   chan int
	}
	type BadFunc struct {
		Name string
		Fn   func()
	}

	if !isJSONSerializable(reflect.TypeOf(Good{})) {
		t.Fatal("Good struct should be serializable")
	}
	if !isJSONSerializable(reflect.TypeOf(WithIgnored{})) {
		t.Fatal("struct with json:\"-\" chan field should be serializable")
	}
	if isJSONSerializable(reflect.TypeOf(Bad{})) {
		t.Fatal("struct with chan field should NOT be serializable")
	}
	if isJSONSerializable(reflect.TypeOf(BadFunc{})) {
		t.Fatal("struct with func field should NOT be serializable")
	}
}

func TestIsJSONSerializable_Collections(t *testing.T) {
	// slice of strings: OK
	if !isJSONSerializable(reflect.TypeOf([]string{})) {
		t.Fatal("[]string should be serializable")
	}
	// map[string]int: OK
	if !isJSONSerializable(reflect.TypeOf(map[string]int{})) {
		t.Fatal("map[string]int should be serializable")
	}
	// map[int]string: NOT OK (non-string key)
	if isJSONSerializable(reflect.TypeOf(map[int]string{})) {
		t.Fatal("map[int]string should NOT be serializable")
	}
	// slice of chan: NOT OK
	if isJSONSerializable(reflect.TypeOf([]chan int{})) {
		t.Fatal("[]chan int should NOT be serializable")
	}
}

func TestIsJSONSerializable_Rejected(t *testing.T) {
	ch := make(chan int)
	if isJSONSerializable(reflect.TypeOf(ch)) {
		t.Fatal("chan should NOT be serializable")
	}
	fn := func() {}
	if isJSONSerializable(reflect.TypeOf(fn)) {
		t.Fatal("func should NOT be serializable")
	}
	c := complex(1, 2)
	if isJSONSerializable(reflect.TypeOf(c)) {
		t.Fatal("complex128 should NOT be serializable")
	}
}

func TestIsJSONSerializable_Nil(t *testing.T) {
	if !isJSONSerializable(nil) {
		t.Fatal("nil type should be considered serializable")
	}
}

func TestIsJSONSerializable_Pointer(t *testing.T) {
	type S struct {
		X int
	}
	if !isJSONSerializable(reflect.TypeOf((*S)(nil))) {
		t.Fatal("*S should be serializable")
	}
}
