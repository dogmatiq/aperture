package ordered

import (
	"fmt"
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
)

var _ dogma.ProjectionEventScope = &scope{}

// scope is an implementation of dogma.ProjectionEventScope.
//
// It is used by Projector when invoking the projection handler.
type scope struct {
	resource   []byte
	offset     uint64
	handler    string
	recordedAt time.Time
	logger     logging.Logger
}

// RecordedAt returns the time at which the event was recorded.
func (s scope) RecordedAt() time.Time {
	return s.recordedAt
}

// Log records an informational message within the context of the message
// that is being handled.
func (s scope) Log(f string, v ...interface{}) {
	logging.Log(
		s.logger,
		"[%s %s@%d] %s",
		s.handler,
		s.resource,
		s.offset,
		fmt.Sprintf(f, v...),
	)
}
