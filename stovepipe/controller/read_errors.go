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

	"github.com/uber/submitqueue/platform/errs"
)

const maxHistoryIdentifierBytes = 255

// ErrInvalidRequest is returned when a request fails validation.
var ErrInvalidRequest = errs.NewUserError(errors.New("invalid request"))

// IsInvalidRequest reports whether err contains an invalid request classification.
func IsInvalidRequest(err error) bool {
	return errors.Is(err, ErrInvalidRequest)
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
