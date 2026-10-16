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

// Package noop provides a queuepause.Store under which no queue is ever paused.
package noop

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/queuepause"
)

var _ queuepause.Store = Store{}

// Store is a queuepause.Store that reports every queue as not paused.
type Store struct{}

// New returns a Store that never pauses a queue.
func New() Store {
	return Store{}
}

// Get always returns the zero state.
func (Store) Get(context.Context, string) (entity.QueuePause, error) {
	return entity.QueuePause{}, nil
}
