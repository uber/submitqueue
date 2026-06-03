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
// as a controller's explicit NewUserError wrap). The package-level Classify
// function owns the walk.
//
// Classifiers are typically stateless; the canonical convention is to expose a
// package-level singleton value (e.g. mysqlerrs.Classifier) rather than a
// constructor.
type Classifier interface {
	Classify(err error) Verdict
}

// Classify is the single, explicit classification pass. It is intended to be
// called exactly once per error chain — typically by the consumer immediately
// after a controller returns — and produces a chain that subsequent IsUserError
// / IsRetryable / IsDependencyError calls can interpret with simple type
// checks (no further classifier walks).
//
// Semantics:
//
//   - nil in, nil out.
//   - If err's chain already carries a framework classification (*userError or
//     *infraError anywhere in the chain), returns err unchanged — the chain is
//     already interpretable by IsUserError / IsRetryable / IsDependencyError.
//   - Otherwise, walks the chain from outermost to innermost, asking each
//     classifier per node. The FIRST non-Unknown verdict wins; the outermost
//     such node determines the wrap. err is wrapped with the framework
//     constructor matching that verdict (User -> NewUserError, InfraRetryable
//     -> NewRetryableError, etc.) and the wrapped error is returned.
//   - Verdict Infra means "non-retryable infra" — which is already the default
//     behavior for an unwrapped chain, so no wrap is added.
//   - If no classifier recognises anything, err is returned unchanged.
//
// Implementation: two passes over the chain. Pass 1 is a cheap type check
// looking for an existing framework wrap and short-circuits if one is found —
// no classifier is invoked. Pass 2 runs the configured classifiers per node.
// Walking the chain is cheap relative to a classifier call, so this avoids
// running classifiers whenever the chain is already classified deeper down.
//
// NOTE: this central classifier model cannot disambiguate errors of the same
// underlying type produced by different extensions (e.g. a net.OpError from a
// mysql connection vs the same type from an HTTP caller would both match the
// mysql classifier here). Resolving that requires per-extension provenance
// tagging; intentionally deferred.
func Classify(err error, classifiers ...Classifier) error {
	if err == nil {
		return nil
	}

	// Pass 1 — cheap framework-wrap check. If any node already carries a
	// framework type, the chain is interpretable as-is and classifiers are
	// never invoked.
	for cur := err; cur != nil; cur = errors.Unwrap(cur) {
		switch cur.(type) {
		case *userError, *infraError:
			return err
		}
	}

	// Pass 2 — run classifiers per node from outermost to innermost. Stop at
	// the first non-Unknown verdict.
	var verdict Verdict
	for cur := err; cur != nil && verdict == Unknown; cur = errors.Unwrap(cur) {
		for _, c := range classifiers {
			if v := c.Classify(cur); v != Unknown {
				verdict = v
				break
			}
		}
	}

	switch verdict {
	case User:
		return NewUserError(err)
	case InfraRetryable:
		return NewRetryableError(err)
	case InfraDependency:
		return NewDependencyError(err)
	case InfraDependencyRetryable:
		return NewRetryableDependencyError(err)
	}
	// Unknown or Infra — no wrap needed; the existing chain already behaves as
	// non-retryable infra at the IsRetryable / IsUserError layer.
	return err
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
