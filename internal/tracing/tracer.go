package tracing

import (
	"context"

	"go.opentelemetry.io/otel/api/trace"
)

var noop trace.NoopTracer

// WithSpan calls t.WithSpan(ctx, op, fn).
//
// If t is nil or NoopTracer, the tracer from ctx is used instead.
func WithSpan(
	ctx context.Context,
	t trace.Tracer,
	op string,
	fn func(context.Context) error,
) error {
	if t == nil || t == noop {
		t = trace.SpanFromContext(ctx).Tracer()
	}

	return t.WithSpan(ctx, op, fn)
}
