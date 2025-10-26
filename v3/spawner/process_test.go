package spawner

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3/diagnostics"
)

type recordingReporter struct {
	events []diagnostics.Event
}

func (r *recordingReporter) Report(evt diagnostics.Event) {
	r.events = append(r.events, evt)
}

func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	println("helper stdout")
	_, _ = os.Stderr.WriteString("helper stderr\n")
	os.Exit(0)
}

func TestProcessSpawnerLaunch(t *testing.T) {
	reporter := &recordingReporter{}
	cfg := Config{
		BinaryPath: os.Args[0],
		Args:       []string{"-test.run=TestHelperProcess", "--"},
		Env:        []string{"GO_WANT_HELPER_PROCESS=1"},
		Reporter:   reporter,
	}
	s := NewProcessSpawner(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	handle, err := s.Launch(ctx, LaunchConfig{Address: "127.0.0.1:1234"})
	if err != nil {
		t.Fatalf("launch: %v", err)
	}

	if err := handle.Wait(ctx); err != nil {
		t.Fatalf("wait: %v", err)
	}

	if len(reporter.events) == 0 {
		t.Fatalf("expected diagnostics events from helper process")
	}
	hasOutput := false
	for _, evt := range reporter.events {
		if _, ok := evt.Metadata["line"]; ok {
			hasOutput = true
			break
		}
	}
	if !hasOutput {
		t.Fatalf("expected output diagnostics, got %#v", reporter.events)
	}
}

func TestProcessSpawnerSetReporter(t *testing.T) {
	cfg := Config{
		BinaryPath: os.Args[0],
		Args:       []string{"-test.run=TestHelperProcess", "--"},
		Env:        []string{"GO_WANT_HELPER_PROCESS=1"},
	}
	s := NewProcessSpawner(cfg)

	reporter := &recordingReporter{}
	s.SetReporter(reporter)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	handle, err := s.Launch(ctx, LaunchConfig{Address: "127.0.0.1:4321"})
	if err != nil {
		t.Fatalf("launch: %v", err)
	}
	if err := handle.Wait(ctx); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if len(reporter.events) == 0 {
		t.Fatalf("expected diagnostics events from helper process via set reporter")
	}
}
