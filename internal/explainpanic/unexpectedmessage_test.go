package explainpanic_test

import (
	. "github.com/dogmatiq/aperture/internal/explainpanic"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/dogma/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func UnexpectedMessage()", func() {
	It("provides more context to UnexpectedMessage panic values", func() {
		Expect(func() {
			UnexpectedMessage(
				&fixtures.ProjectionMessageHandler{},
				"Method",
				fixtures.MessageE1,
				func() {
					panic(dogma.UnexpectedMessage)
				},
			)
		}).To(PanicWith("*fixtures.ProjectionMessageHandler.Method() panicked due to an unexpected message of type fixtures.MessageE"))
	})

	It("does not interfere with other panic values", func() {
		Expect(func() {
			UnexpectedMessage(
				&fixtures.ProjectionMessageHandler{},
				"Method",
				fixtures.MessageE1,
				func() {
					panic("<panic>")
				},
			)
		}).To(PanicWith("<panic>"))
	})

	It("does not panic if fn() does not panic", func() {
		Expect(func() {
			UnexpectedMessage(
				&fixtures.ProjectionMessageHandler{},
				"Method",
				fixtures.MessageE1,
				func() {},
			)
		}).NotTo(Panic())
	})
})
