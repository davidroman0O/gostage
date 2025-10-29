package orchestrator

import "testing"

func TestValidateRemoteBindingSuccess(t *testing.T) {
	binding := &RemoteBinding{
		Spawner: &SpawnerBinding{
			Name: "remote",
			Cfg: SpawnerConfig{
				Name:      "remote",
				AuthToken: "secret",
				TLS: TLSFiles{
					CertPath: "cert.pem",
					KeyPath:  "key.pem",
					CAPath:   "ca.pem",
				},
			},
		},
	}
	if err := ValidateRemoteBinding(binding); err != nil {
		t.Fatalf("expected validation to succeed, got %v", err)
	}
}
