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

// BatchStatus is the state of a validation batch as it moves through the pipeline.
type BatchStatus string

const (
	// BatchStatusUnknown is the unreachable default. It should never be seen in the system.
	BatchStatusUnknown BatchStatus = ""
	// BatchStatusPending means the batch has been created but speculate has not yet run.
	BatchStatusPending BatchStatus = "pending"
	// BatchStatusBuilding means the batch is moving through the speculate→build→buildsignal→bisect cycle.
	BatchStatusBuilding BatchStatus = "building"
	// BatchStatusSucceeded means all commits in the batch's range have been validated green.
	BatchStatusSucceeded BatchStatus = "succeeded"
	// BatchStatusFailed means an offending commit in the range has been isolated and marked failed.
	BatchStatusFailed BatchStatus = "failed"
)

// IsBatchStatusTerminal returns true if the batch has reached a final state.
func IsBatchStatusTerminal(s BatchStatus) bool {
	return s == BatchStatusSucceeded || s == BatchStatusFailed
}

// Batch is a contiguous range of trunk commits submitted for validation together.
// The range spans from FromSHA (oldest, inclusive) to ToSHA (newest, inclusive)
// and represents all commits since the last known green on the branch.
// Bisection creates sub-range batches from the same type — there is no separate
// bisection entity; the state of the search lives in the ordinary batch results.
type Batch struct {
	// ID is the unique identifier for this batch.
	ID string
	// FromSHA is the oldest commit SHA in the validation range (inclusive).
	FromSHA string
	// ToSHA is the newest commit SHA in the validation range (inclusive).
	ToSHA string
	// Repository is the repository this batch validates.
	Repository string
	// Branch is the branch this batch validates.
	Branch string
	// Status is the current state of this batch.
	Status BatchStatus
	// Version is incremented on each update and used for optimistic locking.
	// Version arithmetic lives in the controller; the store performs a pure conditional write.
	Version int32
	// CreatedAt is the time this batch was created, in milliseconds since epoch.
	CreatedAt int64
	// UpdatedAt is the time this batch was last updated, in milliseconds since epoch.
	UpdatedAt int64
}
