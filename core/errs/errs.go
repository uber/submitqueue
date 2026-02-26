package errs

import (
	"errors"
)

// userError represents an error caused by invalid user input or actions.
// Use NewUserError or NewRetryableUserError to wrap an underlying cause.
type userError struct {
	// cause is the underlying error.
	cause error
	// retryable indicates whether the operation can be retried.
	retryable bool
}

// NewUserError creates a non-retryable user error wrapping the given cause.
// A user error is an error that is caused by the user's action or input, for example an invalid input or a merge conflict.
func NewUserError(cause error) error {
	return &userError{cause: cause, retryable: false}
}

// NewRetryableUserError creates a retryable user error wrapping the given cause. It is very rare for the user
// error to be retryable and often a result of misclassification. Please know what you are doing when you use this.
func NewRetryableUserError(cause error) error {
	return &userError{cause: cause, retryable: true}
}

// Error returns the error message.
func (e *userError) Error() string {
	return e.cause.Error()
}

// Unwrap returns the underlying cause for errors.Is/As compatibility.
func (e *userError) Unwrap() error {
	return e.cause
}

// Is supports errors.Is matching by framework type. Any *userError in the
// chain matches any *userError target, enabling type-based matching through
// the IsUserError helper.
func (e *userError) Is(target error) bool {
	_, ok := target.(*userError)
	return ok
}

// infraError represents an error caused by infrastructure failures such as
// network issues, database outages, or service unavailability.
// Use NewRetryableError to explicitly mark an infra error as retryable.
//
// Any error that is not explicitly a userError is classified as an infra
// error by convention. Only errors wrapped with NewRetryableError are
// retryable; all other non-user errors are non-retryable by default.
type infraError struct {
	// cause is the underlying error.
	cause error
	// retryable indicates whether the operation can be retried.
	retryable bool
	// dependency indicates whether the error originated in a downstream
	// dependency (e.g. an external service or database, typically executed out of the box).
	dependency bool
}

// NewRetryableError creates a retryable infra error wrapping the given cause.
func NewRetryableError(cause error) error {
	return &infraError{cause: cause, retryable: true}
}

// NewDependencyError creates a non-retryable dependency infra error
// wrapping the given cause. A dependency error is an error that is caused by a downstream dependency
// outside the control of the current system, for example an external build system being down.
func NewDependencyError(cause error) error {
	return &infraError{cause: cause, dependency: true}
}

// NewRetryableDependencyError creates a retryable dependency infra error
// wrapping the given cause. A retryable dependency error is an error that is caused by a downstream dependency
// outside the control of the current system, for example an external build system being down.
func NewRetryableDependencyError(cause error) error {
	return &infraError{cause: cause, retryable: true, dependency: true}
}

// Error returns the error message.
func (e *infraError) Error() string {
	return e.cause.Error()
}

// Unwrap returns the underlying cause for errors.Is/As compatibility.
func (e *infraError) Unwrap() error {
	return e.cause
}

// Is supports errors.Is matching by framework type. Any *infraError in the
// chain matches any *infraError target, enabling type-based matching through
// the IsError helper.
func (e *infraError) Is(target error) bool {
	_, ok := target.(*infraError)
	return ok
}

// IsUserError checks if err is or wraps a user error.
func IsUserError(err error) bool {
	var target *userError
	return errors.As(err, &target)
}

// IsRetryable checks if err is retryable. Returns true only when err is or
// wraps an error whose retryable flag is set. A generic
// error (not wrapped) returns false, consistent
// with the convention that unclassified errors are non-retryable.
func IsRetryable(err error) bool {
	var ue *userError
	if errors.As(err, &ue) {
		return ue.retryable
	}
	var ie *infraError
	if errors.As(err, &ie) {
		return ie.retryable
	}
	return false
}

// IsDependencyError checks if err is or wraps an infra error that originated
// in a downstream dependency. Returns false for user errors, generic errors,
// and infra errors not marked as dependency.
func IsDependencyError(err error) bool {
	var ie *infraError
	if errors.As(err, &ie) {
		return ie.dependency
	}
	return false
}
