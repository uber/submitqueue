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

import "fmt"

const (
	maxQueueIdentifierBytes   = 235
	maxStorageIdentifierBytes = 255
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
