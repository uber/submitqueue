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

// BatchChanges is the normalized, batch-level view of all changes in a batch,
// assembled by the score controller and handed to a Scorer. A Batch references
// only request IDs, so the controller resolves each request's change records and
// flattens their details into Changes — giving the scorer the whole batch's
// change facts in one value without coupling it to storage.
type BatchChanges struct {
	// BatchID is the batch being scored. Format: "<queue>/batch/<counter_value>".
	BatchID string
	// Queue is the queue the batch belongs to.
	Queue string
	// Changes is every change (URI + provider-supplied details) across all requests
	// in the batch. Order is unspecified.
	Changes []ChangeInfo
}

// TotalLinesChanged returns the total number of lines touched across every change in the batch.
func (b BatchChanges) TotalLinesChanged() int {
	total := 0
	for _, c := range b.Changes {
		total += c.Details.TotalLinesChanged()
	}
	return total
}

// TotalFiles returns the total number of files touched across every change in the batch.
func (b BatchChanges) TotalFiles() int {
	total := 0
	for _, c := range b.Changes {
		total += c.Details.FileCount()
	}
	return total
}
