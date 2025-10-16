package testkit

import (
	"testing"

	"github.com/davidroman0O/gostage/v3/registry"
)

// ResetRegistry installs a fresh safe registry for each test to avoid
// leaking definitions across suites.
func ResetRegistry(t *testing.T) {
	t.Helper()
	registry.SetDefault(registry.NewSafeRegistry())
}
