package ordered_test

import (
	"context"
	"time"

	. "github.com/dogmatiq/aperture/ordered"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"golang.org/x/sync/errgroup"
)

var _ = Describe("type MemoryStream", func() {
	var (
		now    time.Time
		ctx    context.Context
		cancel func()
		stream *MemoryStream
	)

	BeforeEach(func() {
		now = time.Now()

		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		stream = &MemoryStream{
			StreamID: "<id>",
		}

		stream.Append(
			now,
			MessageA1,
			MessageB1,
			MessageA2,
			MessageB2,
		)
	})

	AfterEach(func() {
		cancel()
	})

	Describe("func ID()", func() {
		It("returns the stream ID", func() {
			Expect(stream.ID()).To(Equal("<id>"))
		})
	})

	Describe("func Open()", func() {
		It("honours the initial offset", func() {
			cur, err := stream.Open(ctx, 2, nil)
			Expect(err).ShouldNot(HaveOccurred())
			defer cur.Close()

			env, err := cur.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(env).To(Equal(
				Envelope{
					2,
					now,
					MessageA2,
				},
			))

			env, err = cur.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(env).To(Equal(
				Envelope{
					3,
					now,
					MessageB2,
				},
			))
		})

		It("applies the message type filter", func() {
			cur, err := stream.Open(ctx, 0, []dogma.Message{MessageA{}})
			Expect(err).ShouldNot(HaveOccurred())
			defer cur.Close()

			env, err := cur.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(env).To(Equal(
				Envelope{
					0,
					now,
					MessageA1,
				},
			))

			env, err = cur.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(env).To(Equal(
				Envelope{
					2,
					now,
					MessageA2,
				},
			))
		})
	})

	Describe("func Append()", func() {
		It("wakes waiting consumers", func() {
			g, ctx := errgroup.WithContext(ctx)
			barrier := make(chan struct{})

			fn := func() error {
				defer GinkgoRecover()

				cur, err := stream.Open(ctx, 4, []dogma.Message{MessageB{}})
				if err != nil {
					return err
				}
				defer cur.Close()

				barrier <- struct{}{}
				env, err := cur.Next(ctx)
				if err != nil {
					return err
				}

				Expect(env).To(Equal(
					Envelope{
						5,
						now,
						MessageB3,
					},
				))

				return nil
			}

			g.Go(fn)
			g.Go(fn)

			<-barrier
			<-barrier
			stream.Append(now, MessageA3)
			stream.Append(now, MessageB3)

			err := g.Wait()
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	Describe("func Truncate()", func() {
		It("truncates events before the given offset", func() {
			stream.Truncate(2)

			cur, err := stream.Open(ctx, 1, nil)
			Expect(err).ShouldNot(HaveOccurred())

			_, err = cur.Next(ctx)
			Expect(err).To(MatchError("can not read truncated event at offset 1, the first available offset is 2"))
		})

		It("does not truncate events after the given offset", func() {
			stream.Truncate(2)

			cur, err := stream.Open(ctx, 2, nil)
			Expect(err).ShouldNot(HaveOccurred())

			env, err := cur.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(env).To(Equal(
				Envelope{
					2,
					now,
					MessageA2,
				},
			))

			env, err = cur.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(env).To(Equal(
				Envelope{
					3,
					now,
					MessageB2,
				},
			))
		})

		It("returns the number of truncated events", func() {
			n := stream.Truncate(2)
			Expect(n).To(BeNumerically("==", 2))
			n = stream.Truncate(3)
			Expect(n).To(BeNumerically("==", 1))
		})

		It("does not truncate any events if they have already been truncated", func() {
			n := stream.Truncate(2)
			Expect(n).To(BeNumerically("==", 2))
			n = stream.Truncate(2)
			Expect(n).To(BeNumerically("==", 0))
		})

		It("allows truncation up to the next offset", func() {
			Expect(func() {
				stream.Truncate(4)
			}).NotTo(Panic())
		})

		It("panics if the offset is greater than the total number of events", func() {
			Expect(func() {
				stream.Truncate(5)
			}).To(Panic())
		})
	})

	Describe("type memoryCursor", func() {
		Describe("func Next()", func() {
			It("returns an error if the cursor is already closed", func() {
				cur, err := stream.Open(ctx, 4, []dogma.Message{MessageB{}})
				Expect(err).ShouldNot(HaveOccurred())

				cur.Close()

				_, err = cur.Next(ctx)
				Expect(err).Should(HaveOccurred())
			})

			It("returns an error if the cursor is closed while waiting", func() {
				cur, err := stream.Open(ctx, 4, []dogma.Message{MessageB{}})
				Expect(err).ShouldNot(HaveOccurred())

				barrier := make(chan struct{})
				go func() {
					time.Sleep(100 * time.Millisecond)
					cur.Close()
					close(barrier)
				}()

				_, err = cur.Next(ctx)
				<-barrier
				Expect(err).Should(HaveOccurred())
			})

			It("returns an error if the context is canceled while waiting", func() {
				cur, err := stream.Open(ctx, 4, []dogma.Message{MessageB{}})
				Expect(err).ShouldNot(HaveOccurred())
				defer cur.Close()

				barrier := make(chan struct{})
				go func() {
					time.Sleep(100 * time.Millisecond)
					cancel()
					close(barrier)
				}()

				_, err = cur.Next(ctx)
				<-barrier
				Expect(err).Should(HaveOccurred())
			})
		})
	})
})
