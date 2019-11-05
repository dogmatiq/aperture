package eventsourcing

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
//
// This can be overridden for specific handlers or messages by implementing
// dogma.ProjectionMessageHandler.TimeoutHint().
const DefaultTimeout = 1 * time.Second

// Projector reads events from a stream and applies them to a projection.
type Projector struct {
	Stream  Stream
	Handler dogma.ProjectionMessageHandler
	Log     func(string, ...interface{})

	config   *config.ProjectionConfig
	resource []byte
	current  []byte
	next     []byte
}

// Run applies events to a projection until ctx is canceled or an error occurs.
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
		[]byte(env.StreamName),
		p.current,
		p.next,
		scope{
			recordedAt: env.RecordedAt,
			log:        p.Log,
		},
		env.Message,
	)

	p.current, p.next = p.next, p.current

	return ok, err
}

// withTimeout returns a context with a deadline computed from the handler's
// timeout hint for the given message, or from defaultTimeout if the handler
// does not provide a hint.
func (p *Projector) withTimeout(ctx context.Context, m dogma.Message) (context.Context, func()) {
	t := p.Handler.TimeoutHint(m)
	if t == 0 {
		t = DefaultTimeout
	}

	return context.WithTimeout(ctx, t)
}
