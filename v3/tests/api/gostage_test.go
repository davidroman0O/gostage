package gostage_test

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	gostage "github.com/davidroman0O/gostage/v3"
	"github.com/davidroman0O/gostage/v3/internal/gostagetest"
	"github.com/davidroman0O/gostage/v3/state"
	statetest "github.com/davidroman0O/gostage/v3/state/testkit"
	_ "modernc.org/sqlite"
)

type stringerValue struct{}

func (stringerValue) String() string { return "stringer" }

type badJSON struct{}

func (badJSON) MarshalJSON() ([]byte, error) { return nil, fmt.Errorf("boom") }

func TestPoolMetadataToStrings(t *testing.T) {
	cases := []struct {
		name   string
		input  map[string]any
		expect map[string]string
	}{
		{
			name:   "nil map",
			input:  nil,
			expect: nil,
		},
		{
			name: "string passthrough",
			input: map[string]any{
				"region": "us-central",
			},
			expect: map[string]string{
				"region": "us-central",
			},
		},
		{
			name: "numeric json",
			input: map[string]any{
				"slots": 3,
			},
			expect: map[string]string{
				"slots": "3",
			},
		},
		{
			name: "struct json",
			input: map[string]any{
				"config": map[string]any{"env": "dev", "count": 2},
			},
			expect: map[string]string{
				"config": `{"count":2,"env":"dev"}`,
			},
		},
		{
			name: "stringer",
			input: map[string]any{
				"value": stringerValue{},
			},
			expect: map[string]string{
				"value": "stringer",
			},
		},
		{
			name: "nil values dropped",
			input: map[string]any{
				"keep": "value",
				"drop": nil,
			},
			expect: map[string]string{
				"keep": "value",
			},
		},
	}

	for _, tc := range cases {
		if got := gostagetest.PoolMetadataToStrings(tc.input); !reflect.DeepEqual(got, tc.expect) {
			t.Fatalf("%s: expected %v, got %v", tc.name, tc.expect, got)
		}
	}
}

func TestPoolMetadataToStringsPanicsOnEncodeError(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic when metadata encoding fails")
		}
	}()

	gostagetest.PoolMetadataToStrings(map[string]any{"bad": badJSON{}})
}

func TestRunWithSQLiteExistingDBPreservesPragmas(t *testing.T) {
	t.Helper()

	tempDir := t.TempDir()
	dbPath := fmt.Sprintf("file:%s/test.db?mode=rwc", tempDir)

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if _, err := db.Exec("PRAGMA foreign_keys=OFF"); err != nil {
		t.Fatalf("set foreign_keys: %v", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout=4321"); err != nil {
		t.Fatalf("set busy_timeout: %v", err)
	}
	if _, err := db.Exec("PRAGMA journal_mode=DELETE"); err != nil {
		t.Fatalf("set journal_mode: %v", err)
	}

	queue := state.NewMemoryQueue()
	store := state.NewMemoryStore()
	observer := statetest.NewCaptureObserver()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, diag, err := gostage.Run(ctx,
		gostage.WithQueue(queue),
		gostage.WithStore(store),
		gostage.WithStateObserver(observer),
		gostage.WithStateReader(observer.Reader()),
		gostage.WithSQLite(gostage.SQLiteConfig{
			DB:              db,
			ApplyMigrations: false,
			DisableWAL:      true,
		}),
	)
	if err != nil {
		t.Fatalf("run with existing db: %v", err)
	}

	done := make(chan struct{})
	go func() {
		for range diag {
		}
		close(done)
	}()

	cancel()
	if err := node.Close(); err != nil {
		t.Fatalf("close node: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("diagnostics channel not closed in time")
	}

	if val := pragmaInt(t, db, "foreign_keys"); val != 0 {
		t.Fatalf("foreign_keys changed unexpectedly: got %d", val)
	}
	if val := pragmaInt(t, db, "busy_timeout"); val != 4321 {
		t.Fatalf("busy_timeout changed unexpectedly: got %d", val)
	}
	if mode := pragmaText(t, db, "journal_mode"); mode != "delete" {
		t.Fatalf("journal_mode changed unexpectedly: got %s", mode)
	}
}

func pragmaInt(t *testing.T, db *sql.DB, pragma string) int {
	t.Helper()
	row := db.QueryRow("PRAGMA " + pragma)
	var value int
	if err := row.Scan(&value); err != nil {
		t.Fatalf("scan pragma %s: %v", pragma, err)
	}
	return value
}

func pragmaText(t *testing.T, db *sql.DB, pragma string) string {
	t.Helper()
	row := db.QueryRow("PRAGMA " + pragma)
	var value string
	if err := row.Scan(&value); err != nil {
		t.Fatalf("scan pragma %s: %v", pragma, err)
	}
	return value
}
