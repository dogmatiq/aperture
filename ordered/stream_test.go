package ordered_test

import (
	"context"
	"time"

	. "github.com/dogmatiq/aperture/ordered"
	"github.com/dogmatiq/dogma"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"golang.org/x/sync/errgroup"
)

type typeA string
type typeB string

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
			typeA("<message 1>"),
			typeB("<message 2>"),
			typeA("<message 3>"),
			typeB("<message 4>"),
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
					typeA("<message 3>"),
				},
			))

			env, err = cur.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(env).To(Equal(
				Envelope{
					3,
					now,
					typeB("<message 4>"),
				},
			))
		})

		It("applies the message type filter", func() {
			cur, err := stream.Open(ctx, 0, []dogma.Message{typeA("")})
			Expect(err).ShouldNot(HaveOccurred())
			defer cur.Close()

			env, err := cur.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(env).To(Equal(
				Envelope{
					0,
					now,
					typeA("<message 1>"),
				},
			))

			env, err = cur.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(env).To(Equal(
				Envelope{
					2,
					now,
					typeA("<message 3>"),
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

				cur, err := stream.Open(ctx, 4, []dogma.Message{typeB("")})
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
						typeB("<message 6>"),
					},
				))

				return nil
			}

			g.Go(fn)
			g.Go(fn)

			<-barrier
			<-barrier
			stream.Append(now, typeA("<message 5>"))
			stream.Append(now, typeB("<message 6>"))

			err := g.Wait()
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	Describe("type memoryCursor", func() {
		Describe("func Next()", func() {
			It("returns an error if the cursor is already closed", func() {
				cur, err := stream.Open(ctx, 4, []dogma.Message{typeB("")})
				Expect(err).ShouldNot(HaveOccurred())

				cur.Close()

				_, err = cur.Next(ctx)
				Expect(err).Should(HaveOccurred())
			})

			It("returns an error if the cursor is closed while waiting", func() {
				cur, err := stream.Open(ctx, 4, []dogma.Message{typeB("")})
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
				cur, err := stream.Open(ctx, 4, []dogma.Message{typeB("")})
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
