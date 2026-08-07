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

//go:generate mockgen -source=speculation_path_set_store.go -destination=mock/speculation_path_set_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// SpeculationPathSetStore persists one head batch's chosen speculation paths.
//
// A set is keyed by its head batch ID within the bound queue and versioned as a
// whole: every path in it shares that head, and the set is the unit of both
// replacement and optimistic locking. There is no lookup by anything but the
// head — callers that need a queue's live sets enumerate the heads from the
// batch listing they already hold and read each set by key.
type SpeculationPathSetStore interface {
	// Get retrieves a head's path set, where head is the head batch's ID.
	// Returns ErrNotFound if the head has no set yet, which is the normal state
	// for a batch nothing has speculated on.
	Get(ctx context.Context, head string) (entity.SpeculationPathSet, error)

	// Create stores a head's first path set.
	// Returns ErrAlreadyExists if the head already has one.
	Create(ctx context.Context, set entity.SpeculationPathSet) error

	// Update replaces the stored set with set and writes newVersion, but only if
	// the persisted version still matches oldVersion. If it does not, returns
	// ErrVersionMismatch and writes nothing.
	//
	// The whole entity goes in rather than the fields being changed: a set is
	// replaced wholesale, so this is a conditional put on a key — the primitive
	// every backend offers directly, instead of a field-level update each
	// non-SQL backend would have to emulate with a read-modify-write.
	//
	// set.Version is ignored. oldVersion is the guard and newVersion is the
	// value written, so version arithmetic stays with the caller: compute
	// newVersion, call, and assign it to the in-memory set only once this
	// returns nil.
	Update(ctx context.Context, set entity.SpeculationPathSet, oldVersion, newVersion int32) error
}
