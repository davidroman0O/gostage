package telemetry

// ChannelSink forwards events to a buffered channel for observation in tests.
type ChannelSink struct {
	ch chan Event
}

func NewChannelSink(buffer int) *ChannelSink {
	if buffer <= 0 {
		buffer = 64
	}
	return &ChannelSink{ch: make(chan Event, buffer)}
}

func (s *ChannelSink) Record(evt Event) {
	s.ch <- evt
}

func (s *ChannelSink) C() <-chan Event { return s.ch }
