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

import "sync"

// State stores ephemeral modeled execution results by production operation ID.
type State[T any] struct {
	mu     sync.Mutex
	values map[string]T
}

// NewState returns an empty thread-safe state store.
func NewState[T any]() *State[T] {
	return &State[T]{values: make(map[string]T)}
}

// Get returns the value stored for key.
func (s *State[T]) Get(key string) (T, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.values[key]
	return value, ok
}

// PutIfAbsent stores value when key is absent and returns the stored value.
func (s *State[T]) PutIfAbsent(key string, value T) (T, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.values[key]; ok {
		return existing, false
	}
	s.values[key] = value
	return value, true
}

// Set stores value for key.
func (s *State[T]) Set(key string, value T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[key] = value
}
