package ordered

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
)

// ErrStreamSealed is returned by Stream.Open() and Cursor.Next() to indicate
// that a stream will never produce any more events.
var ErrStreamSealed = errors.New("stream sealed")

// A Stream is an ordered sequence of event messages.
//
// Stream implementations may optionally allow for streams to be marked as
// "sealed", indicating that no new messages will appear on the stream.
type Stream interface {
	// ID returns a unique identifier for the stream.
	//
	// The tuple of stream ID and event offset must uniquely identify a message.
	ID() string

	// Open returns a cursor used to read events from this stream.
	//
	// offset is the position of the first event to read. The first event on a
	// stream is always at offset 0. If the given offset is beyond the end of a
	// sealed stream, ErrStreamSealed is returned.
	//
	// filter is a set of zero-value event messages, the types of which indicate
	// which event types are returned by Cursor.Next(). If filter is empty, all
	// events types are returned.
	Open(ctx context.Context, offset uint64, filter []dogma.Message) (Cursor, error)
}

// A Cursor reads events from a stream.
//
// Cursors are not intended to be used by multiple goroutines concurrently.
type Cursor interface {
	// Next returns the next relevant event in the stream.
	//
	// If the end of the stream is reached it blocks until a relevant event is
	// appended to the stream, ctx is canceled or the stream is sealed. If the
	// stream is sealed, ErrStreamSealed is returned.
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

// MemoryStream is an implementation of Stream that stores messages in-memory.
//
// It is intended primarily for testing.
type MemoryStream struct {
	// StreamID is a unique identifier for the stream, it must not be empty.
	// The tuple of stream ID and event offset must uniquely identify a message.
	StreamID string

	m        sync.RWMutex
	ready    chan struct{}
	first    uint64
	next     uint64
	sealed   bool
	messages []Envelope
}

// ID returns a unique identifier for the stream.
//
// The tuple of stream ID and event offset must uniquely identify a message.
func (s *MemoryStream) ID() string {
	if s.StreamID == "" {
		panic("stream ID must not be empty")
	}

	return s.StreamID
}

// Open returns a cursor used to read events from this stream.
//
// offset is the position of the first event to read. The first event on a
// stream is always at offset 0. If the given offset is beyond the end of a
// sealed stream, ErrStreamSealed is returned.
//
// filter is a set of zero-value event messages, the types of which indicate
// which event types are returned by Cursor.Next(). If filter is empty, all
// events types are returned.
func (s *MemoryStream) Open(
	ctx context.Context,
	offset uint64,
	filter []dogma.Message,
) (Cursor, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	if s.sealed && offset >= s.next {
		return nil, ErrStreamSealed
	}

	c := &memoryCursor{
		stream: s,
		offset: offset,
		closed: make(chan struct{}),
	}

	if len(filter) > 0 {
		c.filter = message.TypesOf(filter...)
	}

	return c, nil
}

// Append appends messages to the end of the stream.
//
// It panics if the stream is sealed.
func (s *MemoryStream) Append(t time.Time, messages ...dogma.Message) {
	for _, m := range messages {
		if m == nil {
			panic("can not append nil messages")
		}
	}

	s.m.Lock()
	defer s.m.Unlock()

	if s.sealed {
		panic("can not append to sealed stream")
	}

	for _, m := range messages {
		env := Envelope{s.next, t, m}
		s.next++
		s.messages = append(s.messages, env)
	}

	if s.ready != nil {
		close(s.ready)
		s.ready = nil
	}
}

// Truncate discards any events before the given offset.
//
// It returns the number of truncated events.
//
// It panics if the offset is greater than the total number of events appended
// to the stream.
func (s *MemoryStream) Truncate(offset uint64) uint64 {
	s.m.Lock()
	defer s.m.Unlock()

	if offset > s.next {
		panic(fmt.Sprintf(
			"can not truncate stream to offset %d, next offset is %d",
			offset,
			s.next,
		))
	}

	count := offset - s.first

	if count <= 0 {
		return 0
	}

	s.first = offset
	s.messages = s.messages[count:]

	return count
}

// Seal marks the stream as sealed, preventing new events from being appended.
func (s *MemoryStream) Seal() {
	s.m.Lock()
	defer s.m.Unlock()

	if s.sealed {
		return
	}

	s.sealed = true

	if s.ready != nil {
		close(s.ready)
		s.ready = nil
	}
}

type memoryCursor struct {
	stream    *MemoryStream
	offset    uint64
	filter    message.TypeSet
	closeOnce sync.Once
	closed    chan struct{}
}

var errCursorClosed = errors.New("cursor is closed")

// Next returns the next relevant event in the stream.
//
// If the end of the stream is reached it blocks until a relevant event is
// appended to the stream, ctx is canceled or the stream is sealed. If the
// stream is sealed, ErrStreamSealed is returned.
func (c *memoryCursor) Next(ctx context.Context) (Envelope, error) {
	for {
		select {
		case <-ctx.Done():
			return Envelope{}, ctx.Err()
		case <-c.closed:
			return Envelope{}, errCursorClosed
		default:
		}

		env, ready, err := c.get()

		if err != nil || ready == nil {
			return env, err
		}

		select {
		case <-ctx.Done():
			return Envelope{}, ctx.Err()
		case <-c.closed:
			return Envelope{}, errCursorClosed
		case <-ready:
		}
	}
}

// Close stops the cursor.
//
// Any current or future calls to Next() return a non-nil error.
func (c *memoryCursor) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
	})

	return nil
}

func (c *memoryCursor) get() (Envelope, <-chan struct{}, error) {
	c.stream.m.Lock()
	defer c.stream.m.Unlock()

	if c.offset < c.stream.first {
		return Envelope{}, nil, fmt.Errorf(
			"can not read truncated event at offset %d, the first available offset is %d",
			c.offset,
			c.stream.first,
		)
	}

	for c.stream.next > c.offset {
		env := c.stream.messages[c.offset-c.stream.first]
		c.offset++

		if c.filter != nil && !c.filter.HasM(env.Message) {
			continue
		}

		return env, nil, nil
	}

	if c.stream.sealed {
		return Envelope{}, nil, ErrStreamSealed
	}

	if c.stream.ready == nil {
		c.stream.ready = make(chan struct{})
	}

	return Envelope{}, c.stream.ready, nil
}
