package consumer

import (
	"errors"
	"fmt"
)

// NonRetryableError indicates a poison pill message that should not be retried.
// When a controller returns this error, the consumer will ack the message (removing it
// from the queue) instead of nacking it for retry. Use this for permanently malformed
// messages that will never succeed regardless of retry count.
type NonRetryableError struct {
	// Cause is the underlying error that caused the message to be non-retryable.
	Cause error
}

// NewNonRetryableError creates a new NonRetryableError wrapping the given cause.
func NewNonRetryableError(cause error) *NonRetryableError {
	return &NonRetryableError{Cause: cause}
}

// Error returns the error message.
func (e *NonRetryableError) Error() string {
	return fmt.Sprintf("non-retryable: %v", e.Cause)
}

// Unwrap returns the underlying cause for errors.Is/As compatibility.
func (e *NonRetryableError) Unwrap() error {
	return e.Cause
}

// IsNonRetryable checks if an error is or wraps a NonRetryableError.
func IsNonRetryable(err error) bool {
	var target *NonRetryableError
	return errors.As(err, &target)
}
