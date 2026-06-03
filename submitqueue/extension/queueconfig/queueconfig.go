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

package queueconfig

//go:generate mockgen -source=queueconfig.go -destination=mock/queueconfig_mock.go -package=mock

import (
	"context"
	"errors"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// ErrNotFound is returned when the requested queue configuration does not exist.
var ErrNotFound = errors.New("queue config not found")

// Store loads and provides queue configurations.
// Implementations may read from YAML files, databases, remote services, etc.
type Store interface {
	// Get returns the configuration for a named queue.
	// Returns ErrNotFound if no configuration exists for the given name.
	Get(ctx context.Context, name string) (entity.QueueConfig, error)

	// List returns all configured queues.
	List(ctx context.Context) ([]entity.QueueConfig, error)
}
