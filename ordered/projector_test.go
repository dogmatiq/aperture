package ordered_test

import (
	"context"
	"errors"
	"time"

	. "github.com/dogmatiq/aperture/ordered"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/otel/api/metric"
)

var _ = Describe("type Projector", func() {
	var (
		meter   metric.NoopMeter
		now     time.Time
		ctx     context.Context
		cancel  func()
		stream  *MemoryStream
		handler *ProjectionMessageHandler
		logger  *logging.BufferedLogger
		proj    *Projector
	)

	BeforeEach(func() {
		now = time.Now()

		// note that we use a test timeout that is greater than the package's
		// default timeout, so we can test that it is being applied correctly.
		ctx, cancel = context.WithTimeout(context.Background(), DefaultTimeout*2)

		stream = &MemoryStream{
			StreamID: "<id>",
		}

		stream.Append(
			now,
			MessageA1,
			MessageB1,
			MessageA2,
			MessageB2,
			MessageA3,
			MessageB3,
		)

		handler = &ProjectionMessageHandler{
			ConfigureFunc: func(c dogma.ProjectionConfigurer) {
				c.Identity("<proj>", "<proj-key>")
				c.ConsumesEventType(MessageA{})
			},
		}

		logger = &logging.BufferedLogger{}

		handleTimeMeasure := meter.NewFloat64Measure("")
		conflictCount := meter.NewInt64Counter("")
		offsetGauge := meter.NewInt64Gauge("")

		proj = &Projector{
			Stream:  stream,
			Handler: handler,
			Logger:  logger,
			Metrics: &ProjectorMetrics{
				HandleTimeMeasure: handleTimeMeasure.Bind(nil),
				ConflictCount:     conflictCount.Bind(nil),
				OffsetGauge:       offsetGauge.Bind(nil),
			},
		}
	})

	AfterEach(func() {
		cancel()

		proj.Metrics.HandleTimeMeasure.Unbind()
		proj.Metrics.ConflictCount.Unbind()
		proj.Metrics.OffsetGauge.Unbind()
	})

	Describe("func Run()", func() {
		It("passes the filtered events to the projection in order", func() {
			var messages []dogma.Message
			handler.HandleEventFunc = func(
				_ context.Context,
				_, _, _ []byte,
				_ dogma.ProjectionEventScope,
				m dogma.Message,
			) (bool, error) {
				messages = append(messages, m)

				if len(messages) == 3 {
					cancel()
				}

				return true, nil
			}

			err := proj.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
			Expect(messages).To(Equal(
				[]dogma.Message{
					MessageA1,
					MessageA2,
					MessageA3,
				},
			))
		})

		It("uses the timeout hint from the handler", func() {
			handler.TimeoutHintFunc = func(dogma.Message) time.Duration {
				return 100 * time.Millisecond
			}

			handler.HandleEventFunc = func(
				ctx context.Context,
				_, _, _ []byte,
				_ dogma.ProjectionEventScope,
				_ dogma.Message,
			) (bool, error) {
				dl, ok := ctx.Deadline()
				Expect(ok).To(BeTrue())
				Expect(dl).To(BeTemporally("~", time.Now().Add(100*time.Millisecond)))
				cancel()
				return true, nil
			}

			err := proj.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("falls back to the projector's default timeout", func() {
			proj.DefaultTimeout = 500 * time.Millisecond

			handler.HandleEventFunc = func(
				ctx context.Context,
				_, _, _ []byte,
				_ dogma.ProjectionEventScope,
				_ dogma.Message,
			) (bool, error) {
				dl, ok := ctx.Deadline()
				Expect(ok).To(BeTrue())
				Expect(dl).To(BeTemporally("~", time.Now().Add(500*time.Millisecond)))
				cancel()
				return true, nil
			}

			err := proj.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("falls back to the global default timeout", func() {
			handler.HandleEventFunc = func(
				ctx context.Context,
				_, _, _ []byte,
				_ dogma.ProjectionEventScope,
				_ dogma.Message,
			) (bool, error) {
				dl, ok := ctx.Deadline()
				Expect(ok).To(BeTrue())
				Expect(dl).To(BeTemporally("~", time.Now().Add(DefaultTimeout)))
				cancel()
				return true, nil
			}

			err := proj.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("returns an error if the handler returns an error", func() {
			handler.HandleEventFunc = func(
				ctx context.Context,
				_, _, _ []byte,
				_ dogma.ProjectionEventScope,
				_ dogma.Message,
			) (bool, error) {
				return false, errors.New("<error>")
			}

			err := proj.Run(ctx)
			Expect(err).To(MatchError(
				"unable to consume from '<id>' for the '<proj>' projection: <error>",
			))
		})

		It("returns an error if the handler configuration is invalid", func() {
			handler.ConfigureFunc = nil
			err := proj.Run(ctx)
			Expect(err).To(MatchError(
				"*fixtures.ProjectionMessageHandler is configured without an identity, Identity() must be called exactly once within Configure()",
			))
		})

		It("returns if the context is canceled", func() {
			done := make(chan error)
			go func() {
				done <- proj.Run(ctx)
			}()

			cancel()
			err := <-done
			Expect(err).To(Equal(context.Canceled))
		})

		Context("scope", func() {
			It("exposes the time that the event was recorded", func() {
				handler.HandleEventFunc = func(
					_ context.Context,
					_, _, _ []byte,
					s dogma.ProjectionEventScope,
					_ dogma.Message,
				) (bool, error) {
					Expect(s.RecordedAt()).To(BeTemporally("==", now))
					cancel()
					return true, nil
				}

				err := proj.Run(ctx)
				Expect(err).To(Equal(context.Canceled))
			})

			It("logs messages to the logger", func() {
				handler.HandleEventFunc = func(
					_ context.Context,
					_, _, _ []byte,
					s dogma.ProjectionEventScope,
					_ dogma.Message,
				) (bool, error) {
					s.Log("format %s", "<value>")
					cancel()
					return true, nil
				}

				err := proj.Run(ctx)
				Expect(err).To(Equal(context.Canceled))

				Expect(logger.Messages()).To(ContainElement(
					logging.BufferedLogMessage{
						Message: "[<proj> <id>@0] format <value>",
					},
				))
			})
		})

		Context("optimistic concurrency control", func() {
			It("starts consuming from the next offset", func() {
				handler.ResourceVersionFunc = func(
					_ context.Context,
					res []byte,
				) ([]byte, error) {
					Expect(res).To(Equal([]byte("<id>")))
					return []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02}, nil
				}

				handler.HandleEventFunc = func(
					_ context.Context,
					_, _, _ []byte,
					_ dogma.ProjectionEventScope,
					m dogma.Message,
				) (bool, error) {
					Expect(m).To(Equal(MessageA3))
					cancel()
					return true, nil
				}

				err := proj.Run(ctx)
				Expect(err).To(Equal(context.Canceled))
			})

			It("passes the correct resource and versions to the handler", func() {
				handler.ResourceVersionFunc = func(
					_ context.Context,
					res []byte,
				) ([]byte, error) {
					return []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02}, nil
				}

				handler.HandleEventFunc = func(
					_ context.Context,
					r, c, n []byte,
					_ dogma.ProjectionEventScope,
					_ dogma.Message,
				) (bool, error) {
					Expect(r).To(Equal([]byte("<id>")))
					Expect(c).To(Equal([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02}))
					Expect(n).To(Equal([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04}))
					cancel()
					return true, nil
				}

				err := proj.Run(ctx)
				Expect(err).To(Equal(context.Canceled))
			})

			It("passes the correct resource and versions to the handler when the resource does not exist", func() {
				handler.ResourceVersionFunc = func(
					_ context.Context,
					res []byte,
				) ([]byte, error) {
					return nil, nil
				}

				handler.HandleEventFunc = func(
					_ context.Context,
					r, c, n []byte,
					_ dogma.ProjectionEventScope,
					_ dogma.Message,
				) (bool, error) {
					Expect(r).To(Equal([]byte("<id>")))
					Expect(c).To(BeEmpty())
					Expect(n).To(Equal([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}))
					cancel()
					return true, nil
				}

				err := proj.Run(ctx)
				Expect(err).To(Equal(context.Canceled))
			})

			It("restarts the consumer when a conflict occurs", func() {
				handler.HandleEventFunc = func(
					_ context.Context,
					_, _, _ []byte,
					_ dogma.ProjectionEventScope,
					_ dogma.Message,
				) (bool, error) {
					handler.ResourceVersionFunc = func(
						_ context.Context,
						res []byte,
					) ([]byte, error) {
						return []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02}, nil
					}

					handler.HandleEventFunc = func(
						_ context.Context,
						_, _, _ []byte,
						_ dogma.ProjectionEventScope,
						m dogma.Message,
					) (bool, error) {
						Expect(m).To(Equal(MessageA3))
						cancel()
						return true, nil
					}

					return false, nil
				}

				err := proj.Run(ctx)
				Expect(err).To(Equal(context.Canceled))
			})

			It("returns an error if the current version is malformed", func() {
				handler.ResourceVersionFunc = func(
					context.Context,
					[]byte,
				) ([]byte, error) {
					return []byte{00}, nil
				}

				err := proj.Run(ctx)
				Expect(err).To(MatchError(
					"unable to consume from '<id>' for the '<proj>' projection: version is 1 byte(s), expected 0 or 8",
				))
			})

			It("returns an error if the current version can not be read", func() {
				handler.ResourceVersionFunc = func(
					context.Context,
					[]byte,
				) ([]byte, error) {
					return nil, errors.New("<error>")
				}

				err := proj.Run(ctx)
				Expect(err).To(MatchError(
					"unable to consume from '<id>' for the '<proj>' projection: <error>",
				))
			})
		})
	})
})
