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

// Package fake provides an in-memory queuepause.Store for tests and examples.
// Seed or change a queue's state with Set; a queue never set is not paused.
// FailWith injects a read error on every Get to exercise the error path without
// a real backend. Safe for concurrent use, so a test can change state while a
// consumer is reading. It is intended for tests and examples only, never
// production.
package fake

import (
	"context"
	"sync"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/queuepause"
)

var _ queuepause.Store = (*Store)(nil)

// Store is a programmable in-memory queuepause.Store.
type Store struct {
	mu     sync.RWMutex
	states map[string]entity.QueuePause
	err    error
}

// New returns a Store under which no queue is paused.
func New() *Store {
	return &Store{states: map[string]entity.QueuePause{}}
}

// Set replaces the pause state of a queue. Setting the zero state unpauses it.
func (s *Store) Set(queue string, state entity.QueuePause) *Store {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[queue] = state
	return s
}

// FailWith makes every Get return err. Passing nil restores normal reads.
func (s *Store) FailWith(err error) *Store {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
	return s
}

// Get returns the state last set for the queue, or the zero state if none was.
func (s *Store) Get(_ context.Context, queue string) (entity.QueuePause, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.err != nil {
		return entity.QueuePause{}, s.err
	}
	return s.states[queue], nil
}
