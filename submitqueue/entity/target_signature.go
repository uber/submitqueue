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

package entity

// TargetSignature is the set of build targets a batch changes, as 64-bit IDs.
// The whole record is immutable: (Queue, BatchID) is its identity and there is
// no update path.
type TargetSignature struct {
	// Queue is the queue the batch belongs to. Unique together with BatchID.
	Queue string

	// BatchID is the canonical decimal batch identifier, unique within Queue.
	BatchID string

	// BaseRevision is the commit SHA the changed targets were computed against.
	BaseRevision string

	// Targets is sorted ascending by ID, with no duplicate IDs.
	Targets []SignatureTarget

	// CreatedAt is the Unix milliseconds timestamp when the record was created.
	CreatedAt int64
}

// SignatureTarget is one changed build target in a TargetSignature.
type SignatureTarget struct {
	// ID is the 64-bit hash of the target's label.
	ID uint64

	// Distance is the distance from the nearest directly changed source in the
	// reverse dependency graph; 0 means directly changed.
	Distance int32
}
