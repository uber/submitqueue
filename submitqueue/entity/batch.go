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

import "encoding/json"

// BatchState defines the possible states of a batch.
type BatchState string

const (
	// BatchStateUnknown is the unreachable state. It is set by default when the structure is initialized. It should never be seen in the system.
	BatchStateUnknown BatchState = ""
	// BatchStateCreated is the state of a batch that has been created for processing.
	BatchStateCreated BatchState = "created"
	// BatchStateSpeculating is the state of a batch that is undergoing speculative execution.
	BatchStateSpeculating BatchState = "speculating"
	// BatchStateMerging is the state of a batch that is being merged after speculative execution.
	BatchStateMerging BatchState = "merging"
	// BatchStateSucceeded is the terminal state of a batch that has been successfully landed.
	BatchStateSucceeded BatchState = "succeeded"
	// BatchStateFailed is the terminal state of a batch that has failed.
	BatchStateFailed BatchState = "failed"
	// BatchStateScored is the state of a batch that has been scored for build success probability.
	BatchStateScored BatchState = "scored"
	// BatchStateCancelling is the non-terminal intent state set when a cancel has been requested but the
	// batch has not yet been transitioned to BatchStateCancelled. A batch in this state may still reach
	// BatchStateSucceeded or BatchStateFailed if a concurrent merge wins the race (e.g. the push had
	// already completed before the cancel CAS observed the batch); those terminal states prevail.
	// Forward-progress controllers must treat this state as halted (no new work). The speculate
	// controller owns the transition to the terminal BatchStateCancelled and the downstream fan-out
	// (cancelling in-flight builds, respeculating dependents, publishing to conclude).
	BatchStateCancelling BatchState = "cancelling"
	// BatchStateCancelled is the terminal state of a batch that was cancelled before completion.
	BatchStateCancelled BatchState = "cancelled"
)

// IsTerminal returns true if the batch state is a terminal state.
// Terminal states are states from which no further transitions are possible.
// BatchStateCancelling is intentionally excluded: cancellation is best-effort and a Cancelling batch
// may still transition to BatchStateSucceeded or BatchStateFailed before it reaches BatchStateCancelled.
// Callers that want to gate forward progress (and treat Cancelling as halted) should use
// IsBatchStateHalted instead.
func (s BatchState) IsTerminal() bool {
	switch s {
	case BatchStateSucceeded, BatchStateFailed, BatchStateCancelled:
		return true
	default:
		return false
	}
}

// IsBatchStateHalted returns true if the batch is either terminal or in the process of being cancelled.
// Forward-progress controllers (score, build, buildsignal, speculate, merge) use this to short-circuit
// work for batches that the user has asked to cancel — even though Cancelling is non-terminal, no
// further pipeline work should start (cancel will write the terminal state and fan out).
func IsBatchStateHalted(s BatchState) bool {
	return s.IsTerminal() || s == BatchStateCancelling
}

// Batch represents a group of requests to land (merge into target branch of the source control repository).
type Batch struct {
	// ID is the globally unique identifier for the batch. Format: "<queue>/batch/<counter_value>".
	ID string

	// Queue is the name of the queue processing the land request. Queue name is defined in the configuration and should be unique within the system.
	Queue string

	// Contains is a list of land request IDs that are part of this batch.
	// Request IDs will always be part of the same queue.
	//
	// For e.g. - [queueA/1, queueA/2, queueA/3].
	//
	Contains []string

	// Dependencies is a list of other batch IDs that this batch depends on.
	// Dependencies will always be part of the same queue. This way batches form a directed acyclic graph (DAG).
	// If a batch A depends on batch B directly, it means that some request in batch A has overlapping changed targets with
	// some another request in batch B. The Dependencies list contains all the transitive closure of all the dependencies, both direct and indirect.
	// The order is not specified. Only active batches are considered for dependencies, i.e. if the batch is in a terminal state, it does not need to be included.
	// Because batch states are eventually consistent, dependent batches identified at the time of batch creation may move to terminal states. The interpretation logic
	// should reconcile batch states separately (i.e. ignore them for processing).
	//
	//This field is ok to be updated whether the state of the dependency graph changes. Update should use Version property for optimistic locking.
	//
	// Example: consider batches - queueA/batch/1, queueA/batch/2, queueA/batch/3
	// such that - queueA/batch/2 and queueA/batch/3 have overlapping targets with requests in queueA/batch/1, but queueA/batch/2 and queueA/batch/3 do not have overlapping targets with each other.
	//
	// In this case, the Dependencies field for -
	// - queueA/batch/1 will be empty
	// - queueA/batch/2 will contain queueA/batch/1
	// - queueA/batch/3 will contain queueA/batch/1
	Dependencies []string

	// Score is the predicted probability of build success for this batch, ranging from 0.0 to 1.0.
	// Set during the scoring phase. Zero value means the batch has not been scored yet.
	Score float64

	// The state of the batch lifecycle this batch is in. Updateable field with Version for optimistic locking.
	State BatchState

	// Version is the version of the object. It is used for optimistic locking.
	// Versioning starts at 1 and is incremented for each change to the object.
	Version int32
}

// ToBytes serializes the Batch to JSON bytes for queue message payload.
func (b Batch) ToBytes() ([]byte, error) {
	return json.Marshal(b)
}

// BatchFromBytes deserializes a Batch from JSON bytes.
func BatchFromBytes(data []byte) (Batch, error) {
	var batch Batch
	err := json.Unmarshal(data, &batch)
	return batch, err
}

// BatchID is a lightweight entity for publishing and consuming just the batch identifier via the queue.
type BatchID struct {
	// ID is the globally unique identifier for the batch.
	ID string `json:"id"`
}

// ToBytes serializes the BatchID to JSON bytes for queue message payload.
func (b BatchID) ToBytes() ([]byte, error) {
	return json.Marshal(b)
}

// BatchIDFromBytes deserializes a BatchID from JSON bytes.
func BatchIDFromBytes(data []byte) (BatchID, error) {
	var bid BatchID
	err := json.Unmarshal(data, &bid)
	return bid, err
}
