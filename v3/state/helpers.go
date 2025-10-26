package state

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

func cloneTimePointer(ts *time.Time) *time.Time {
	if ts == nil {
		return nil
	}
	val := *ts
	return &val
}

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

func toOptionalNullDuration(d *time.Duration) sql.NullInt64 {
	if d == nil || *d <= 0 {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(*d), Valid: true}
}

func boolToInt64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func toOptionalNullBool(b *bool) sql.NullInt64 {
	if b == nil {
		return sql.NullInt64{}
	}
	if *b {
		return sql.NullInt64{Int64: 1, Valid: true}
	}
	return sql.NullInt64{Int64: 0, Valid: true}
}

func toOptionalNullString(val *string) sql.NullString {
	if val == nil {
		return sql.NullString{}
	}
	if *val == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: *val, Valid: true}
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

func isBusyError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "database is locked") || strings.Contains(msg, "SQLITE_BUSY")
}

func retryWhileBusy(ctx context.Context, attempts int, fn func() error) error {
	if attempts <= 0 {
		attempts = 1
	}
	backoff := 10 * time.Millisecond
	var err error
	for i := 0; i < attempts; i++ {
		err = fn()
		if err == nil || !isBusyError(err) {
			return err
		}
		if ctx != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		time.Sleep(backoff)
		if backoff < 200*time.Millisecond {
			backoff *= 2
		}
	}
	return err
}
