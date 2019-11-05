package ordered

import (
	"context"
	"time"

	"github.com/dogmatiq/dogma"
)

// A Stream is an ordered sequence of event messages.
type Stream interface {
	// ID returns a unique identifier for the stream.
	//
	// The tuple of stream ID and event offset must uniquely identify a message.
	ID() string

	// Open returns a cursor used to read events from this stream.
	//
	// offset is the position of the first event to read. The first event
	// on a stream is always at offset 0.
	//
	// types is a set of zero-value event messages, the types of which indicate
	// which event types the cursor exposes.
	Open(ctx context.Context, offset uint64, types []dogma.Message) (Cursor, error)
}

// A Cursor reads events from a stream.
//
// Cursors are not intended to be used by multiple goroutines concurrently.
type Cursor interface {
	// Next returns the next relevant event in the stream.
	//
	// If the end of the stream is reached, it blocks until a relevant event
	// is appended to the stream, or ctx is canceled.
	Next(ctx context.Context) (Envelope, error)

	// Close stops the cursor.
	//
	// Any current or future calls to Next() return a non-nil error.
	Close() error
}

// Envelope is a container for an event on a stream.
type Envelope struct {
	// Offset is the zero-based offset of the message on the stream.
	Offset uint64

	// RecordedAt is the time at which the event occurred.
	RecordedAt time.Time

	// Message is the application-defined message.
	Message dogma.Message
}
