package ordered

import (
	"context"
	"encoding/binary"
	"fmt"
	"reflect"
	"time"

	"github.com/dogmatiq/aperture/internal/tracing"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/message"
	"go.opentelemetry.io/otel/api/core"
	"go.opentelemetry.io/otel/api/metric"
	"go.opentelemetry.io/otel/api/trace"
)

// DefaultTimeout is the default timeout to use when applying an event.
const DefaultTimeout = 3 * time.Second

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

	// HandleTimeMeasure is the metric handle used to record the amount of time
	// spent handling each message, in seconds. If it is nil, no metric is
	// recorded.
	HandleTimeMeasure *metric.Float64MeasureHandle

	// ConflictCount is the metric handle used to record the number of OCC
	// conflicts that occur while attempting to handle messages. If it is nil,
	// no metric is recorded.
	ConflictCount *metric.Int64CounterHandle

	// OffsetGauge is the metric handle used to record last offset that was
	// successfully applied to the projection. If it is nil, no metric is
	// recorded.
	OffsetGauge *metric.Int64GaugeHandle

	// Tracer is used to record tracing spans. If it is nil, no tracing is
	// performed.
	Tracer trace.Tracer

	config     *config.ProjectionConfig
	resource   []byte
	current    []byte
	next       []byte
	nameAttr   core.KeyValue
	keyAttr    core.KeyValue
	streamAttr core.KeyValue
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

	p.nameAttr = tracing.HandlerName.String(cfg.HandlerIdentity.Name)
	p.keyAttr = tracing.HandlerKey.String(cfg.HandlerIdentity.Key)
	p.streamAttr = tracing.StreamID.String(p.Stream.ID())

	for {
		if err := p.consume(ctx); err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return fmt.Errorf(
					"unable to consume from '%s' for the '%s' projection: %w",
					p.Stream.ID(),
					p.config.HandlerIdentity.Name,
					err,
				)
			}
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
		types = append(types, reflect.Zero(t.ReflectType()).Interface())
	}

	var offset uint64

	if err := tracing.WithSpan(
		ctx,
		p.Tracer,
		"query last offset",
		func(ctx context.Context) error {
			span := trace.CurrentSpan(ctx)
			span.SetAttributes(
				p.nameAttr,
				p.keyAttr,
				tracing.HandlerTypeProjectionAttr,
				p.streamAttr,
			)

			var err error
			p.current, err = p.Handler.ResourceVersion(ctx, p.resource)
			if err != nil {
				return err
			}

			switch len(p.current) {
			case 0:
				p.current = make([]byte, 8)
				offset = 0
			case 8:
				offset = binary.BigEndian.Uint64(p.current) + 1
			default:
				return fmt.Errorf(
					"the persisted version is %d byte(s), expected 0 or 8",
					len(p.current),
				)
			}

			span.SetAttributes(
				tracing.StreamOffset.Uint64(offset),
			)

			return nil
		},
	); err != nil {
		return nil, err
	}

	logging.Log(
		p.Logger,
		"[%s %s@%d] started consuming",
		p.config.HandlerIdentity.Name,
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

	binary.BigEndian.PutUint64(p.next, env.Offset)

	ctx, cancel := p.withTimeout(ctx, env.Message)
	defer cancel()

	mt := message.TypeOf(env.Message).String()

	var ok bool
	if err := tracing.WithSpan(
		ctx,
		p.Tracer,
		mt,
		func(ctx context.Context) error {
			trace.CurrentSpan(ctx).SetAttributes(
				p.nameAttr,
				p.keyAttr,
				tracing.HandlerTypeProjectionAttr,
				p.streamAttr,
				tracing.StreamOffset.Uint64(env.Offset),
				tracing.MessageType.String(mt),
				tracing.MessageRoleEventAttr,
				tracing.MessageDescription.String(message.Description(env.Message)),
				tracing.MessageRecordedAt.String(env.RecordedAt.Format(time.RFC3339Nano)),
			)

			var err error
			start := time.Now()
			ok, err = p.Handler.HandleEvent(
				ctx,
				p.resource,
				p.current,
				p.next,
				scope{
					resource:   p.resource,
					offset:     env.Offset,
					handler:    p.config.HandlerIdentity.Name,
					recordedAt: env.RecordedAt,
					logger:     p.Logger,
				},
				env.Message,
			)
			if p.HandleTimeMeasure != nil {
				p.HandleTimeMeasure.Record(ctx, time.Since(start).Seconds())
			}

			return err
		},
	); err != nil {
		return false, err
	}

	if ok {
		p.current, p.next = p.next, p.current

		if p.OffsetGauge != nil {
			p.OffsetGauge.Set(ctx, int64(env.Offset))
		}

		return true, nil
	}

	logging.Log(
		p.Logger,
		"[%s %s@%d] an optimisitic concurrency conflict occurred, restarting the consumer",
		p.config.HandlerIdentity.Name,
		p.resource,
		env.Offset,
	)

	if p.ConflictCount != nil {
		p.ConflictCount.Add(ctx, 1)
	}

	return false, nil
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
