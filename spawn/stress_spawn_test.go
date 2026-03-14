package spawn

import (
	"testing"
)

func TestStress_SpawnDepthLimit(t *testing.T) {
	t.Setenv("GOSTAGE_SPAWN_DEPTH", "10")

	env := BuildChildEnv("secret", "token")

	var foundDepth string
	for _, entry := range env {
		const prefix = "GOSTAGE_SPAWN_DEPTH="
		if len(entry) > len(prefix) && entry[:len(prefix)] == prefix {
			foundDepth = entry[len(prefix):]
			break
		}
	}
	if foundDepth != "11" {
		t.Fatalf("expected GOSTAGE_SPAWN_DEPTH=11, got %q", foundDepth)
	}
}

func TestStress_SpawnServerRapidStartStop(t *testing.T) {
	for i := 0; i < 10; i++ {
		runner := NewRunner()
		if err := runner.Close(); err != nil {
			t.Fatalf("iteration %d: Close: %v", i, err)
		}
	}
}
