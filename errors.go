package tap

import (
	"fmt"
	"os"
)

// ErrorHandler is an interface for draining errors to an exceptions aggregator.
type ErrorHandler interface {
	Notify(v interface{}) error
}

// NullErrorHandler is an ErrorHandler implementation that logs the error to Stderr.
type NullErrorHandler struct{}

// Notify implements the ErrorHandler Notify method.
func (h *NullErrorHandler) Notify(v interface{}) error {
	_, err2 := fmt.Fprintf(os.Stderr, "%v\n", v)
	return err2
}
