package ordered

import (
	"fmt"
	"time"

	"github.com/dogmatiq/dodeca/logging"
)

// eventScope is an implementation of dogma.ProjectionEventScope.
type eventScope struct {
	resource   []byte
	offset     uint64
	handler    string
	recordedAt time.Time
	logger     logging.Logger
}

// RecordedAt returns the time at which the event was recorded.
func (s eventScope) RecordedAt() time.Time {
	return s.recordedAt
}

// Log records an informational message within the context of the message
// that is being handled.
func (s eventScope) Log(f string, v ...interface{}) {
	logging.Log(
		s.logger,
		"[%s %s@%d] %s",
		s.handler,
		s.resource,
		s.offset,
		fmt.Sprintf(f, v...),
	)
}

// compactScope is an implementation of dogma.ProjectionCompactScope.
type compactScope struct {
	handler string
	logger  logging.Logger
}

// Log records an informational message within the context of the message
// that is being handled.
func (s compactScope) Log(f string, v ...interface{}) {
	logging.Log(
		s.logger,
		"[%s compact] %s",
		s.handler,
		fmt.Sprintf(f, v...),
	)
}

// Now returns the current time.
func (s compactScope) Now() time.Time {
	return time.Now()
}
