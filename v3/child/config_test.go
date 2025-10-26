package child

import (
	"encoding/json"
	"testing"
)

func TestDetectParsesPoolsFromEnv(t *testing.T) {
	pools := []PoolSpec{
		{
			Name:  "remote",
			Slots: 2,
			Tags:  []string{"gpu"},
			Metadata: map[string]string{
				"zone": "us-east",
			},
		},
	}
	payload, err := json.Marshal(pools)
	if err != nil {
		t.Fatalf("marshal pools: %v", err)
	}
	env := map[string]string{
		EnvMode:          ModeChild,
		EnvParentAddress: "127.0.0.1:12345",
		EnvChildPools:    string(payload),
	}
	cfg, ok, err := Detect(nil, func(key string) string { return env[key] })
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if !ok {
		t.Fatal("expected child mode")
	}
	if len(cfg.Pools) != 1 {
		t.Fatalf("expected 1 pool, got %d", len(cfg.Pools))
	}
	got := cfg.Pools[0]
	if got.Name != "remote" || got.Slots != 2 {
		t.Fatalf("unexpected pool %+v", got)
	}
	if len(got.Tags) != 1 || got.Tags[0] != "gpu" {
		t.Fatalf("unexpected tags %+v", got.Tags)
	}
	if got.Metadata["zone"] != "us-east" {
		t.Fatalf("unexpected metadata %+v", got.Metadata)
	}
}
