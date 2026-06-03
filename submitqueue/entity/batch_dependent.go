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

package entity

// BatchDependent is the reverse index of Batch.Dependencies. While Batch.Dependencies lists the batches a batch depends on (upstream),
// BatchDependent maps a batch to the batches that depend on it (downstream). This enables efficient fan-out notifications
// when a batch completes or fails — rather than scanning all batches to find which ones reference a given dependency,
// the system can look up dependents directly.
//
// Example: consider batches queueA/batch/1, queueA/batch/2, queueA/batch/3
// where batch/2 and batch/3 both depend on batch/1 (i.e. batch/1 is in their Dependencies list).
//
// The BatchDependent records would be:
//   - BatchID=queueA/batch/1 → Dependents=[queueA/batch/2, queueA/batch/3]
//   - BatchID=queueA/batch/2 → Dependents=[] (nothing depends on it)
//   - BatchID=queueA/batch/3 → Dependents=[] (nothing depends on it)
type BatchDependent struct {
	// BatchID is the globally unique identifier of the upstream batch. Format: "<queue>/batch/<counter_value>".
	BatchID string

	// Dependents is a list of batch IDs that depend on this batch (i.e. batches whose Dependencies list contains BatchID).
	// Updated as new batches are created that conflict with this batch. Uses Version for optimistic locking on updates.
	Dependents []string

	// Version is the version of the object. It is used for optimistic locking.
	// Versioning starts at 1 and is incremented for each change to the object.
	Version int32
}
