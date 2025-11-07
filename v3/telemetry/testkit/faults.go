package testkit

import (
    "time"

    "github.com/davidroman0O/gostage/v3/telemetry"
)

// FaultySink sleeps before forwarding an event to the wrapped sink (if any).
// When inner is nil, events are dropped after the delay.
type FaultySink struct {
    Delay time.Duration
    Inner telemetry.Sink
}

func (f FaultySink) Record(evt telemetry.Event) {
    if d := f.Delay; d > 0 { time.Sleep(d) }
    if f.Inner != nil {
        f.Inner.Record(evt)
    }
}

