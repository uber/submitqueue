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

// BatchDependent represents the downstream batches that depend on a given batch.
type BatchDependent struct {
	// BatchID is the globally unique identifier representing a batch.
	BatchID string
	// Dependents is a list of batch IDs that are dependents for this
	// batch.
	//
	// For e.g - Consider batches - queueA/batch/1, queueA/batch/2, queueA/batch/3
	// such that - queueA/batch/2 and queueA/batch/3 depend on queueA/batch/1
	//
	// In this case, the Dependents field for -
	// - queueA/batch/1 will be [queueA/batch/2, queueA/batch/3]
	// - queueA/batch/2 will be empty
	// - queueA/batch/3 will be empty
	//
	Dependents []string
	// Version is the version of the object. It is used for optimistic locking.
	// Versioning starts at 1 and is incremented for each change to the object.
	Version int32
}
