package ordered

import (
	"context"
	"encoding/binary"
	"fmt"
	"reflect"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config"
)

// DefaultTimeout is the default timeout to use when applying an event.
const DefaultTimeout = 3 * time.Second

// Projector reads events from a stream and applies them to a projection.
type Projector struct {
	// Stream is the stream used to obtain event messages.
	Stream Stream

	// Handler is the Dogma projection handler that the messages are applied to.
	Handler dogma.ProjectionMessageHandler

	// Log is a printf-style function used to write log messages about the
	// behavior of the projector. If it is nil no logs are written.
	Log func(string, ...interface{})

	// HandlerLog is a printf-style function used to write application-defined
	// log messages produced within the handler by calling
	// dogma.ProjectionEventScope.Log(). If it is nil no logs are written.
	HandlerLog func(string, ...interface{})

	// DefaultTimeout is the timeout duration to use when hanlding an event if
	// the handler does not provide a timeout hint. If it is zero the global
	// DefaultTimeout constant is used.
	DefaultTimeout time.Duration

	config   *config.ProjectionConfig
	resource []byte
	current  []byte
	next     []byte
}

// Run applies events to a projection until ctx is canceled or an error occurs.
//
// If message handling fails due to an optimistic concurrency conflict within
// the projection the consumer restarts automatically. Any other error is
// returned, in which case it is the caller's responsible to implement any retry
// logic. Run() can safely be called again after exiting with an error.
func (p *Projector) Run(ctx context.Context) error {
	cfg, err := config.NewProjectionConfig(p.Handler)
	if err != nil {
		return err
	}

	p.config = cfg
	p.resource = []byte(p.Stream.ID())
	p.next = make([]byte, 8)

	for {
		if err := p.consume(ctx); err != nil {
			return fmt.Errorf(
				"unable to consume from the '%s' stream for the '%s' (%s) projection: %w",
				p.Stream.ID(),
				p.config.HandlerIdentity.Name,
				p.config.HandlerIdentity.Key,
				err,
			)
		}
	}
}

// consume opens the streams, consumes messages ands applies them to the
// projection.
//
// It consumes until ctx is canceled, and error occurs, or a message is not
// applied due to an OCC conflict, in which case it returns nil.
func (p *Projector) consume(ctx context.Context) error {
	cur, err := p.open(ctx)
	if err != nil {
		return err
	}
	defer cur.Close()

	for {
		ok, err := p.consumeNext(ctx, cur)
		if !ok || err != nil {
			return err
		}
	}
}

// open opens a cursor on the stream based on the offset recorded within the
// projection.
func (p *Projector) open(ctx context.Context) (Cursor, error) {
	var types []dogma.Message
	for t := range p.config.ConsumedMessageTypes() {
		types = append(types, reflect.Zero(t.ReflectType()))
	}

	var err error
	p.current, err = p.Handler.ResourceVersion(ctx, p.resource)
	if err != nil {
		return nil, err
	}

	var offset uint64

	switch len(p.current) {
	case 0:
		p.current = make([]byte, 8)
		offset = 0
	case 8:
		offset = binary.BigEndian.Uint64(p.current) + 1
	default:
		return nil, fmt.Errorf(
			"the persisted version is %d bytes in length, expected 0 or 8",
			len(p.current),
		)
	}

	if p.Log != nil {
		p.Log(
			"[%s %s@%d] started consuming",
			p.config.HandlerIdentity.Name,
			p.resource,
			offset,
		)
	}

	return p.Stream.Open(ctx, offset, types)
}

// consumeNext waits for the next message on the stream then applies it to the
// projection.
func (p *Projector) consumeNext(ctx context.Context, cur Cursor) (bool, error) {
	env, err := cur.Next(ctx)
	if err != nil {
		return false, err
	}

	binary.BigEndian.PutUint64(p.next, env.Offset)

	ctx, cancel := p.withTimeout(ctx, env.Message)
	defer cancel()

	ok, err := p.Handler.HandleEvent(
		ctx,
		p.resource,
		p.current,
		p.next,
		scope{
			resource:   p.resource,
			offset:     env.Offset,
			handler:    p.config.HandlerIdentity.Name,
			recordedAt: env.RecordedAt,
			log:        p.Log,
		},
		env.Message,
	)

	if err != nil {
		return false, err
	}

	if !ok && p.Log != nil {
		p.Log(
			"[%s %s@%d] an optimisitic concurrency conflict occurred, restarting the consumer",
			p.config.HandlerIdentity.Name,
			p.resource,
			env.Offset,
		)
	}

	p.current, p.next = p.next, p.current

	return ok, err
}

// withTimeout returns a context with a deadline computed from the handler's
// timeout hint for the given message, or from defaultTimeout if the handler
// does not provide a hint.
func (p *Projector) withTimeout(ctx context.Context, m dogma.Message) (context.Context, func()) {
	t := p.Handler.TimeoutHint(m)

	if t == 0 {
		t = p.DefaultTimeout
	}

	if t == 0 {
		t = DefaultTimeout
	}

	return context.WithTimeout(ctx, t)
}
