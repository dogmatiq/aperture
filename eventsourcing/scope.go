package eventsourcing

import (
	"time"

	"github.com/dogmatiq/dogma"
)

var _ dogma.ProjectionEventScope = &scope{}

// scope is an implementation of dogma.ProjectionEventScope.
//
// It is used by Projector when invoking the projection handler.
type scope struct {
	recordedAt time.Time
	log        func(string, ...interface{})
}

// RecordedAt returns the time at which the event was recorded.
func (s scope) RecordedAt() time.Time {
	return s.recordedAt
}

// Log records an informational message within the context of the message
// that is being handled.
func (s scope) Log(f string, v ...interface{}) {
	if s.log != nil {
		s.log(f, v...)
	}
}
