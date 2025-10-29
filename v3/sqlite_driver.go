package gostage

// Register the modernc.org/sqlite driver for runtime usage when callers rely on
// gostage.WithSQLite. Without this blank import, go run binaries (such as the
// examples) fail with "sql: unknown driver \"sqlite\"".
import _ "modernc.org/sqlite"
