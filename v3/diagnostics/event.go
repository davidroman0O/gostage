package diagnostics

import "time"

// Severity represents diagnostic importance levels.
type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityError    Severity = "error"
	SeverityCritical Severity = "critical"
)

// Event captures a diagnostic occurrence.
type Event struct {
	OccurredAt time.Time
	Component  string
	Severity   Severity
	Err        error
	Metadata   map[string]any
}
