package explainpanic

import (
	"fmt"

	"github.com/dogmatiq/dogma"
)

// UnexpectedMessage calls fn() and converts dogma.UnexpectedMessage panic
// values to strings that provide more information.
func UnexpectedMessage(
	handler interface{},
	method string,
	m dogma.Message,
	fn func(),
) {
	defer func() {
		v := recover()

		if v == nil {
			return
		}

		if v == dogma.UnexpectedMessage {
			panic(fmt.Sprintf(
				"%T.%s() panicked due to an unexpected message of type %T",
				handler,
				method,
				m,
			))
		}

		panic(v)
	}()

	fn()
}
