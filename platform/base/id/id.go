// Copyright (c) 2026 Uber Technologies, Inc.
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

// Package id formats, validates, and compares counter-generated resource IDs.
package id

import (
	"fmt"
	"strconv"
)

// FromCounter returns the canonical decimal resource ID for value.
func FromCounter(value int64) (string, error) {
	if value <= 0 {
		return "", fmt.Errorf("counter value must be positive")
	}
	return strconv.FormatInt(value, 10), nil
}

// Validate verifies that id is the canonical decimal representation of a positive int64.
func Validate(id string) error {
	value, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return fmt.Errorf("resource ID must be a positive decimal string: %w", err)
	}
	canonical, err := FromCounter(value)
	if err != nil {
		return err
	}
	if id != canonical {
		return fmt.Errorf("resource ID must use canonical decimal form")
	}
	return nil
}

// Compare compares two canonical positive decimal resource IDs numerically.
// Callers must ensure both IDs belong to the same queue and resource kind.
func Compare(a, b string) (int, error) {
	aValue, err := parseResourceID(a)
	if err != nil {
		return 0, fmt.Errorf("invalid first resource ID %q: %w", a, err)
	}
	bValue, err := parseResourceID(b)
	if err != nil {
		return 0, fmt.Errorf("invalid second resource ID %q: %w", b, err)
	}
	switch {
	case aValue < bValue:
		return -1, nil
	case aValue > bValue:
		return 1, nil
	default:
		return 0, nil
	}
}

func parseResourceID(id string) (int64, error) {
	if err := Validate(id); err != nil {
		return 0, err
	}
	value, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return 0, err
	}
	return value, nil
}
