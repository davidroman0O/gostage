package local

import (
	"testing"
	"time"
)

type testBroker struct{}

func (testBroker) Progress(int, string) error                 { return nil }
func (testBroker) Event(string, string, map[string]any) error { return nil }

func TestContextDeadline(t *testing.T) {
	factory := Factory{}
	raw := factory.New(&mockWorkflow{}, &testBroker{})
	impl := raw.(*contextImpl)

	deadline := time.Now().Add(time.Hour)
	impl.SetDeadline(deadline)
	got, ok := impl.Deadline()
	if !ok {
		t.Fatalf("expected deadline")
	}
	if !got.Equal(deadline) {
		t.Fatalf("unexpected deadline: %v", got)
	}
}
