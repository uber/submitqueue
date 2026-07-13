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
)

const (
	maxQueueIdentifierBytes   = 235
	maxStorageIdentifierBytes = 255
	maxChangeRequestResults   = 100
)

func validateQueueIdentifier(queue string) error {
	if queue == "" {
		return fmt.Errorf("queue must be non-empty: %w", ErrInvalidRequest)
	}
	if len(queue) > maxQueueIdentifierBytes {
		return fmt.Errorf("queue exceeds %d bytes: %w", maxQueueIdentifierBytes, ErrInvalidRequest)
	}
	return nil
}

// RequestNotFoundError indicates that no request exists for the selected sqid or change URI.
type RequestNotFoundError struct {
	Sqid      string
	ChangeURI string
}

// Error implements the error interface.
func (e *RequestNotFoundError) Error() string {
	if e.Sqid != "" {
		return fmt.Sprintf("request not found for sqid %q", e.Sqid)
	}
	return fmt.Sprintf("request not found for change URI %q", e.ChangeURI)
}

// IsRequestNotFound returns true if any error in the chain is a *RequestNotFoundError.
func IsRequestNotFound(err error) bool {
	var target *RequestNotFoundError
	return errors.As(err, &target)
}

// TooManyChangeRequestsError indicates that a change URI exceeded the API result limit.
type TooManyChangeRequestsError struct {
	ChangeURI string
	Limit     int
}

// Error implements the error interface.
func (e *TooManyChangeRequestsError) Error() string {
	return fmt.Sprintf("change URI %q matched more than %d requests", e.ChangeURI, e.Limit)
}

// IsTooManyChangeRequests returns true if any error in the chain is a *TooManyChangeRequestsError.
func IsTooManyChangeRequests(err error) bool {
	var target *TooManyChangeRequestsError
	return errors.As(err, &target)
}

// InternalConsistencyError indicates that gateway-owned read models disagree.
type InternalConsistencyError struct {
	Message string
}

// Error implements the error interface.
func (e *InternalConsistencyError) Error() string {
	return e.Message
}

// IsInternalConsistency returns true if any error in the chain is an *InternalConsistencyError.
func IsInternalConsistency(err error) bool {
	var target *InternalConsistencyError
	return errors.As(err, &target)
}

func validateStoredIdentifier(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s must be non-empty: %w", name, ErrInvalidRequest)
	}
	if len(value) > maxStorageIdentifierBytes {
		return fmt.Errorf("%s exceeds %d bytes: %w", name, maxStorageIdentifierBytes, ErrInvalidRequest)
	}
	return nil
}

func validateChangeURIs(changeURIs []string) error {
	if len(changeURIs) == 0 {
		return fmt.Errorf("at least one change URI is required: %w", ErrInvalidRequest)
	}
	seen := make(map[string]struct{}, len(changeURIs))
	for _, changeURI := range changeURIs {
		if err := validateStoredIdentifier("change URI", changeURI); err != nil {
			return err
		}
		if _, ok := seen[changeURI]; ok {
			return fmt.Errorf("duplicate change URI %q: %w", changeURI, ErrInvalidRequest)
		}
		seen[changeURI] = struct{}{}
	}
	return nil
}
