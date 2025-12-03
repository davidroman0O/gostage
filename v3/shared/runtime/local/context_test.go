package local

import (
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3/internal/foundation/clock"
)

type testBroker struct{}

func (testBroker) Progress(int, string) error                 { return nil }
func (testBroker) Event(string, string, map[string]any) error { return nil }

func TestContextDeadline(t *testing.T) {
	testClock := clock.NewTestClock(time.Date(2025, time.January, 2, 12, 0, 0, 0, time.UTC))
	factory := Factory{}
	raw := factory.New(&mockWorkflow{}, &testBroker{})
	impl := raw.(*contextImpl)

	deadline := testClock.Now().Add(time.Hour)
	impl.SetDeadline(deadline)
	got, ok := impl.Deadline()
	if !ok {
		t.Fatalf("expected deadline")
	}
	if !got.Equal(deadline) {
		t.Fatalf("unexpected deadline: %v", got)
	}
}
