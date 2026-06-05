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

// Verdict is the classification of a single error node, returned by a
// Classifier. Unknown means the node carries no signal and the chain walker
// should keep looking; every other value names a terminal classification.
type Verdict int

const (
	// Unknown means this node carries no classification. The chain walker
	// will move on to the next node in the unwrap chain.
	Unknown Verdict = iota
	// User means the error is caused by the user's input or action (e.g. a
	// merge conflict or invalid request) and must not be retried.
	User
	// Infra means a non-retryable infrastructure failure: something below the
	// caller broke in a way that retrying will not fix (e.g. a schema or
	// programmer bug). This is the implicit verdict for an unclassified chain,
	// so Classify does not add a wrap for it.
	Infra
	// InfraRetryable means a transient infrastructure failure that is
	// expected to succeed on retry (e.g. a deadlock, lock-wait timeout, or
	// dropped connection).
	InfraRetryable
	// InfraDependency means a non-retryable failure originating in a
	// downstream dependency outside the caller's control (e.g. an external
	// service rejecting the request).
	InfraDependency
	// InfraDependencyRetryable means a transient failure originating in a
	// downstream dependency (e.g. an external service is briefly unavailable)
	// that is expected to succeed on retry.
	InfraDependencyRetryable
)

// Classifier inspects a single error node (not the whole chain) and returns a
// Verdict. Implementations should return Unknown for nodes they do not
// recognize so the chain walker can continue down the unwrap chain.
//
// Classifiers must not call errors.As / errors.Is themselves, which would walk
// the chain and could shadow a classification carried by an outer node (such
// as a controller's explicit NewUserError wrap). The classifier-based
// ErrorProcessor (see NewClassifierProcessor) owns the walk.
//
// Classifiers are typically stateless; the canonical convention is to expose a
// package-level singleton value (e.g. mysqlerrs.Classifier) rather than a
// constructor.
type Classifier interface {
	Classify(err error) Verdict
}

// IsUserError reports whether err is or wraps a user error, i.e. an error
// produced by NewUserError. Inspects only the framework types in the chain.
func IsUserError(err error) bool {
	var ue *userError
	return errors.As(err, &ue)
}

// IsRetryable reports whether err is or wraps an infra error marked
// retryable, i.e. an error produced by NewRetryableError or
// NewRetryableDependencyError. Inspects only the framework types in the chain.
func IsRetryable(err error) bool {
	var ie *infraError
	return errors.As(err, &ie) && ie.retryable
}

// IsDependencyError reports whether err is or wraps an infra error marked as
// originating in a downstream dependency, i.e. an error produced by
// NewDependencyError or NewRetryableDependencyError. Inspects only the
// framework types in the chain.
func IsDependencyError(err error) bool {
	var ie *infraError
	return errors.As(err, &ie) && ie.dependency
}
