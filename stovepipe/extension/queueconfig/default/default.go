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

// Package default provides a queueconfig.Store that returns global wiring
// defaults for any non-empty queue name. Replace with a YAML- or service-backed
// store when per-queue overrides are needed.
package defaultconfig

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/queueconfig"
)

const (
	_defaultMaxConcurrent   = 1
	_defaultGateWaitDelayMs = 5000
)

// Store is a queueconfig.Store that returns the same defaults for every queue.
type Store struct{}

// NewStore returns a Store backed by global wiring defaults.
func NewStore() Store {
	return Store{}
}

// Get returns the default configuration for any non-empty queue name.
func (Store) Get(_ context.Context, name string) (entity.QueueConfig, error) {
	if name == "" {
		return entity.QueueConfig{}, queueconfig.ErrNotFound
	}
	return entity.QueueConfig{
		Name:            name,
		MaxConcurrent:   _defaultMaxConcurrent,
		GateWaitDelayMs: _defaultGateWaitDelayMs,
	}, nil
}

// List returns an empty slice; this store does not enumerate known queues.
func (Store) List(context.Context) ([]entity.QueueConfig, error) {
	return nil, nil
}
