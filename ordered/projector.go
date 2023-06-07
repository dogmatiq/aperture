package ordered

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/dogmatiq/aperture/internal/explainpanic"
	"github.com/dogmatiq/aperture/ordered/resource"
	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/linger"
	"golang.org/x/sync/errgroup"
)

const (
	// DefaultTimeout is the default timeout to use when applying an event.
	DefaultTimeout = 3 * time.Second

	// DefaultCompactionInterval is the default interval at which a projector
	// will compact its projection.
	DefaultCompactionInterval = 24 * time.Hour

	// DefaultCompactionTimeout is the default timeout to use when compacting a
	// projection.
	DefaultCompactionTimeout = 5 * time.Minute
)

// Projector reads events from a stream and applies them to a projection.
type Projector struct {
	// Stream is the stream used to obtain event messages.
	Stream Stream

	// Handler is the Dogma projection handler that the messages are applied to.
	Handler dogma.ProjectionMessageHandler

	// Logger is the target for log messages from the projector and the handler.
	// If it is nil, logging.DefaultLogger is used.
	Logger logging.Logger

	// DefaultTimeout is the timeout duration to use when hanlding an event if
	// the handler does not provide a timeout hint. If it is zero the global
	// DefaultTimeout constant is used.
	DefaultTimeout time.Duration

	// CompactionInterval is the interval at which the projector compacts the
	// projection. If it is zero the global DefaultCompactionInterval constant
	// is used.
	CompactionInterval time.Duration

	// CompactionTimeout is the default timeout to use when compacting the
	// projection. If it is zero the global DefaultCompactionTimeout is used.
	CompactionTimeout time.Duration

	name     string
	types    message.TypeCollection
	resource []byte
	current  []byte
	next     []byte
}

// Run runs the projection until ctx is canceled or an error occurs.
//
// Event messages are obtained from the stream and passed to the handler for
// handling as they become available. Projection compaction is performed at a
// fixed interval.
//
// If message handling fails due to an optimistic concurrency conflict within
// the projection the consumer restarts automatically.
//
// Run() returns if any other error occurs during handling or compaction, in
// which case it is the caller's responsibility to implement any retry logic.
//
// Run() can safely be called again after exiting with an error.
func (p *Projector) Run(ctx context.Context) (err error) {
	defer configkit.Recover(&err)

	cfg := configkit.FromProjection(p.Handler)

	p.name = cfg.Identity().Name
	p.types = cfg.MessageTypes().Consumed
	p.resource = resource.FromStreamID(p.Stream.ID())

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		for {
			if err := p.compact(gctx); err != nil {
				return fmt.Errorf(
					"unable to compact the '%s' projection: %w",
					p.name,
					err,
				)
			}

			if err := linger.Sleep(
				gctx,
				p.CompactionInterval,
				DefaultCompactionInterval,
			); err != nil {
				return err
			}
		}
	})

	g.Go(func() error {
		for {
			if err := p.consume(gctx); err != nil {
				return fmt.Errorf(
					"unable to consume from '%s' for the '%s' projection: %w",
					p.Stream.ID(),
					p.name,
					err,
				)
			}
		}
	})

	err = g.Wait()

	select {
	case <-ctx.Done():
		// Don't wrap the error at all if we have been asked to bail.
		return ctx.Err()
	default:
		return err
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
	p.types.Range(func(t message.Type) bool {
		types = append(
			types,
			reflect.Zero(t.ReflectType()).Interface().(dogma.Message),
		)
		return true
	})

	var (
		offset uint64
		err    error
	)
	p.current, err = p.Handler.ResourceVersion(ctx, p.resource)
	if err != nil {
		return nil, err
	}

	offset, err = resource.UnmarshalOffset(p.current)
	if err != nil {
		return nil, err
	}

	logging.Log(
		p.Logger,
		"[%s %s@%d] started consuming",
		p.name,
		p.resource,
		offset,
	)

	return p.Stream.Open(ctx, offset, types)
}

// consumeNext waits for the next message on the stream then applies it to the
// projection.
func (p *Projector) consumeNext(ctx context.Context, cur Cursor) (bool, error) {
	env, err := cur.Next(ctx)
	if err != nil {
		return false, err
	}

	if p.next == nil {
		p.next = make([]byte, 8)
	}

	resource.MarshalOffsetInto(p.next, env.Offset+1)

	var hint time.Duration
	explainpanic.UnexpectedMessage(
		p.Handler,
		"TimeoutHint",
		env.Message,
		func() {
			hint = p.Handler.TimeoutHint(env.Message)
		},
	)

	ctx, cancel := linger.ContextWithTimeout(
		ctx,
		hint,
		p.DefaultTimeout,
		DefaultTimeout,
	)
	defer cancel()

	var ok bool
	explainpanic.UnexpectedMessage(
		p.Handler,
		"HandleEvent",
		env.Message,
		func() {
			ok, err = p.Handler.HandleEvent(
				ctx,
				p.resource,
				p.current,
				p.next,
				eventScope{
					resource:   p.resource,
					offset:     env.Offset,
					handler:    p.name,
					recordedAt: env.RecordedAt,
					logger:     p.Logger,
				},
				env.Message,
			)
		},
	)
	if err != nil {
		return false, err
	}

	if ok {
		// keep swapping between the two buffers to avoid repeat allocations
		p.current, p.next = p.next, p.current
		return true, nil
	}

	logging.Log(
		p.Logger,
		"[%s %s@%d] an optimisitic concurrency conflict occurred, restarting the consumer",
		p.name,
		p.resource,
		env.Offset,
	)

	return false, nil
}

// compact calls p.Handler.Compact() with a timeout as per p.CompactionTimeout.
//
// It returns an error if ctx is canceled or some unexpected error occurs. It is
// *not* an error if compaction times out. It is simply retried again at the
// next interval.
func (p *Projector) compact(ctx context.Context) error {
	ctx, cancel := linger.ContextWithTimeout(
		ctx,
		p.CompactionTimeout,
		DefaultCompactionTimeout,
	)
	defer cancel()

	if err := p.Handler.Compact(
		ctx,
		compactScope{
			handler: p.name,
			logger:  p.Logger,
		},
	); err != nil {
		if err != context.DeadlineExceeded {
			// The error was something other than a timeout of the compaction
			// process itself.
			return err
		}

		// Otherwise, the compaction timed out, but this is allowed. Log about
		// it but continue as normal.
		logging.Log(
			p.Logger,
			"[%s compact] %s",
			p.name,
			err,
		)
	}

	return nil
}
