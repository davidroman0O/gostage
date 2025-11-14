package telemetry

// ChannelSink forwards events to a buffered channel for observation in tests.
type ChannelSink struct {
	ch chan Event
}

// NewChannelSink creates a new channel sink with the specified buffer size.
func NewChannelSink(buffer int) *ChannelSink {
	if buffer <= 0 {
		buffer = 64
	}
	return &ChannelSink{ch: make(chan Event, buffer)}
}

// Record records a telemetry event to the channel.
func (s *ChannelSink) Record(evt Event) {
	s.ch <- evt
}

// C returns the channel for receiving telemetry events.
func (s *ChannelSink) C() <-chan Event { return s.ch }
