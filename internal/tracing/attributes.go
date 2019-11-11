package tracing

import (
	"github.com/dogmatiq/enginekit/handler"
	"github.com/dogmatiq/enginekit/message"
	"go.opentelemetry.io/otel/api/key"
)

var (
	// HandlerName is a span attribute key for they name component of a handler's
	// identity.
	HandlerName = key.New("dogma.handler.name")

	// HandlerKey is a span attribute key for they key component of a handler's
	// identity.
	HandlerKey = key.New("dogma.handler.key")

	// HandlerType is a span attribute key for a handler type.
	HandlerType = key.New("dogma.handler.type")

	// MessageType is a span attribute key for the type of a message.
	MessageType = key.New("dogma.message.type")

	// MessageRole is a span attribute key for the role of a message.
	MessageRole = key.New("dogma.message.role")

	// MessageDescription is a span attribute key for the human-readable
	// description of a message.
	MessageDescription = key.New("dogma.message.description")

	// MessageRecordedAt is a span attribute key for the "recorded at" time of
	// an event message.
	MessageRecordedAt = key.New("dogma.message.recorded_at")

	// StreamID is a span attribute key for the ID of an ordered event stream.
	StreamID = key.New("aperture.stream.id")

	// StreamOffset is a span attribute key for the offset of a message on an
	// ordered event stream.
	StreamOffset = key.New("aperture.stream.offset")
)

var (
	// HandlerTypeProjectionAttr is a span attribute with the HandlerType key
	// set to "projection".
	HandlerTypeProjectionAttr = HandlerType.String(handler.ProjectionType.String())

	// MessageRoleEventAttr is a span attribute with the
	// MessageRole key set to "event".
	MessageRoleEventAttr = HandlerType.String(message.EventRole.String())
)
