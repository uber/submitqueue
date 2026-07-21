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

package storage

//go:generate mockgen -source=storage.go -destination=mock/storage_mock.go -package=mock

import (
	"errors"
)

// ErrAlreadyExists is returned by storage implementations when attempting to create a record that already exists.
var ErrAlreadyExists = errors.New("record already exists")

// Storage is a factory interface that aggregates all entity stores into a single injectable dependency.
type Storage interface {
	// GetRequestStore returns the RequestStore instance.
	GetRequestStore() RequestStore

	// GetRequestURIStore returns the RequestURIStore instance.
	GetRequestURIStore() RequestURIStore

	// GetQueueStore returns the QueueStore instance.
	GetQueueStore() QueueStore

	// GetBuildStore returns the BuildStore instance.
	GetBuildStore() BuildStore

	// Close closes the storage and all underlying connections. Should only be called once at the end of the program.
	Close() error
}
