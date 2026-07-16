// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"fmt"
	"sync"
)

// Sequence consumes modeled invocations in declaration order.
type Sequence[T any] struct {
	mu     sync.Mutex
	values []T
	next   int
}

// NewSequence returns a thread-safe sequence containing a copy of values.
func NewSequence[T any](values []T) *Sequence[T] {
	return &Sequence[T]{values: append([]T(nil), values...)}
}

// Next consumes and returns the next value.
func (s *Sequence[T]) Next() (T, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var zero T
	if s.next >= len(s.values) {
		return zero, fmt.Errorf("modeled invocation sequence exhausted")
	}
	value := s.values[s.next]
	s.next++
	return value, nil
}

// Consumed returns the number of values consumed.
func (s *Sequence[T]) Consumed() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.next
}
