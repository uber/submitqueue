// Copyright (c) 2025 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package errs

import (
	"context"
	"errors"
)

// userError represents an error caused by invalid user input or actions.
// User errors are never retryable — only infrastructure errors can be retryable.
// Use NewUserError to wrap an underlying cause.
type userError struct {
	// cause is the underlying error.
	cause error
}

// NewUserError creates a user error wrapping the given cause.
// A user error is an error that is caused by the user's action or input,
// for example an invalid input or a merge conflict. User errors are never
// retryable — only infrastructure errors can be retryable.
func NewUserError(cause error) error {
	return &userError{cause: cause}
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

// IsRetryable checks if err is retryable. Returns true when err is or
// wraps an infrastructure error whose retryable flag is set or when err is context.Canceled. User errors are
// never retryable. A generic error (not wrapped) returns false, consistent
// with the convention that unclassified errors are non-retryable.
func IsRetryable(err error) bool {
	if errors.Is(err, context.Canceled) {
		return true
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
