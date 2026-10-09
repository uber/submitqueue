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

package controller

import (
	"errors"
	"fmt"

	"github.com/uber/submitqueue/platform/base/id"
	"github.com/uber/submitqueue/platform/errs"
)

const maxHistoryIdentifierBytes = 255

// ErrInvalidRequest is returned when a request fails validation.
var ErrInvalidRequest = errs.NewUserError(errors.New("invalid request"))

// IsInvalidRequest reports whether err contains an invalid request classification.
func IsInvalidRequest(err error) bool {
	// Framework wrappers match by classification; compare the underlying sentinel.
	return errors.Is(err, errors.Unwrap(ErrInvalidRequest))
}

func validateHistoryIdentifier(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s must be non-empty: %w", name, ErrInvalidRequest)
	}
	if len(value) > maxHistoryIdentifierBytes {
		return fmt.Errorf("%s exceeds %d bytes: %w", name, maxHistoryIdentifierBytes, ErrInvalidRequest)
	}
	return nil
}

func validateRequestID(value string) error {
	if err := id.Validate(value); err != nil {
		return fmt.Errorf("invalid request ID %q: %v: %w", value, err, ErrInvalidRequest)
	}
	return nil
}

// RequestHistoryByIDNotFoundError indicates that no retained history exists for a request ID.
type RequestHistoryByIDNotFoundError struct {
	// RequestID is the selected request identifier.
	RequestID string
}

// Error implements error.
func (e *RequestHistoryByIDNotFoundError) Error() string {
	return fmt.Sprintf("request history not found for request ID %q", e.RequestID)
}

// RequestHistoryByURINotFoundError indicates that no retained history exists for a URI.
type RequestHistoryByURINotFoundError struct {
	// URI is the selected commit URI.
	URI string
}

// Error implements error.
func (e *RequestHistoryByURINotFoundError) Error() string {
	return fmt.Sprintf("request history not found for URI %q", e.URI)
}

// IsRequestHistoryNotFound reports whether err contains a retained-history absence.
func IsRequestHistoryNotFound(err error) bool {
	var byID *RequestHistoryByIDNotFoundError
	if errors.As(err, &byID) {
		return true
	}
	var byURI *RequestHistoryByURINotFoundError
	return errors.As(err, &byURI)
}

// ListConsistencyError indicates an invalid acceptance mapping, a missing summary,
// or disagreement between the two. It represents an infrastructure failure, not a user lookup miss.
type ListConsistencyError struct {
	// Queue is the selected queue.
	Queue string
	// RequestID identifies the mapped request; empty when the mapping lacks an ID.
	RequestID string
	// Reason describes the inconsistency in the mapping or summary.
	Reason string
	// Err is the underlying summary-read failure, if any.
	Err error
}

func (e *ListConsistencyError) Error() string {
	return fmt.Sprintf("List %s queue=%q request_id=%q", e.Reason, e.Queue, e.RequestID)
}

// Unwrap preserves the underlying summary-read failure, when present.
func (e *ListConsistencyError) Unwrap() error { return e.Err }

// IsListConsistency reports whether err represents inconsistent listing records.
func IsListConsistency(err error) bool {
	var target *ListConsistencyError
	return errors.As(err, &target)
}
