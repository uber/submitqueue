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

// Package storage resolves the orchestrator's queue-scoped stores: the
// pipeline working state behind validation, batching, speculation, building
// and landing. The store contracts themselves stay in
// submitqueue/extension/storage — only the aggregate is service-scoped, so
// what the orchestrator can reach is narrower than what the domain defines.
package storage

//go:generate mockgen -source=storage.go -destination=mock/storage_mock.go -package=mock

import (
	basestorage "github.com/uber/submitqueue/submitqueue/extension/storage"
)

// Config identifies the queue a Storage instance is resolved for. It is the
// shared storage config: an alias rather than a new type, so a caller holding
// one can resolve either service's storage with it.
type Config = basestorage.Config

// Factory resolves the queue-scoped Storage aggregate for a queue. Mirrors the
// extension contract: the host wiring decides which backend serves which
// queue; implementations bind the queue over their backend so a resolved
// instance can only read and write that queue's data.
type Factory interface {
	// For returns the Storage aggregate bound to the queue named in config.
	For(config Config) (Storage, error)
}

// Storage aggregates the orchestrator's queue-scoped stores into a single
// injectable dependency. An instance is resolved per queue through Factory and
// is bound to that queue: entity arguments whose Queue field disagrees with
// the binding are rejected, and reads never surface another queue's records.
type Storage interface {
	// GetRequestStore returns the RequestStore instance.
	GetRequestStore() basestorage.RequestStore

	// GetRequestBatchStore returns the RequestBatchStore instance.
	GetRequestBatchStore() basestorage.RequestBatchStore

	// GetChangeStore returns the ChangeStore instance.
	GetChangeStore() basestorage.ChangeStore

	// GetBatchStore returns the BatchStore instance.
	GetBatchStore() basestorage.BatchStore

	// GetBatchDependentStore returns the BatchDependentStore instance.
	GetBatchDependentStore() basestorage.BatchDependentStore

	// GetQueueBatchStateStore returns the QueueBatchStateStore instance.
	GetQueueBatchStateStore() basestorage.QueueBatchStateStore

	// GetBuildStore returns the BuildStore instance.
	GetBuildStore() basestorage.BuildStore

	// GetSpeculationPathSetStore returns the SpeculationPathSetStore instance.
	GetSpeculationPathSetStore() basestorage.SpeculationPathSetStore

	// GetPathBuildStore returns the PathBuildStore instance.
	GetPathBuildStore() basestorage.PathBuildStore
}
