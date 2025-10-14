package state

import (
	"database/sql"
	"time"
)

func toNullString(val string) sql.NullString {
	if val == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: val, Valid: true}
}

func toNullTime(ts *time.Time) sql.NullTime {
	if ts == nil {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: *ts, Valid: true}
}

func nullableTimeValue(ts time.Time) interface{} {
	if ts.IsZero() {
		return nil
	}
	return ts
}

func toNullDuration(d time.Duration) sql.NullInt64 {
	if d <= 0 {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(d), Valid: true}
}

func boolToInt64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func cloneAnyMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	dup := make(map[string]any, len(src))
	for k, v := range src {
		dup[k] = v
	}
	return dup
}
