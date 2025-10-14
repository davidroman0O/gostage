package local

import (
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3/types"
)

type testBroker struct{}

func (testBroker) Call(types.MessageType, []byte) error                 { return nil }
func (testBroker) Publish(string, []byte, map[string]interface{}) error { return nil }
func (testBroker) Progress(int) error                                   { return nil }
func (testBroker) ProgressCause(string, int) error                      { return nil }
func (testBroker) Log(types.LogLevel, string, ...types.LogField) error  { return nil }

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
