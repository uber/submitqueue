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

import (
	"encoding/json"

	"github.com/uber/submitqueue/entity/change"
	"github.com/uber/submitqueue/entity/mergestrategy"
)

// RequestState defines the possible states of a land request. They are internal and used to implement a state machine. A separate RequestStatus type is used to track the customer-friendly status of a request.
type RequestState string

const (
	// RequestStateUnknown is the unreachable state. It is set by default when the structure is initialized. It should never be seen in the system.
	RequestStateUnknown RequestState = ""
	// RequestStateStarted is the initial state of a land request. It is confirmed by the system but the processing is not started yet.
	RequestStateStarted RequestState = "started"
	// RequestStateValidated indicates that the request has been validated (duplicate check, merge check etc.) successfully.
	RequestStateValidated RequestState = "validated"
	// RequestStateBatched indicates that the request has been claimed by the batch controller and enrolled in a
	// batch. The CAS-write of this state by the batch controller is the serialization point between batch and
	// cancel: the batch controller transitions Validated → Batched immediately before persisting the new batch,
	// so any concurrent cancel that has already transitioned the request to Cancelling will lose the CAS and
	// abandon the batch. From this state forward, the request's terminal outcome is owned by the batch it is
	// enrolled in (via conclude), not by the cancel controller's request-only fast path.
	RequestStateBatched RequestState = "batched"
	// RequestStateProcessing is the state of a land request that is being processed.
	RequestStateProcessing RequestState = "processing"
	// RequestStateLanded is the state of a land request that has been successfully processed and landed. This is the final state.
	RequestStateLanded RequestState = "landed"
	// RequestStateError is the state of a land request that has encountered an error. This is the final state.
	RequestStateError RequestState = "error"
	// RequestStateCancelling is the non-terminal intent state set when the user has requested cancellation but the
	// request has not yet been transitioned to RequestStateCancelled. A request in this state may still reach
	// RequestStateLanded or RequestStateError if a concurrent merge or failure wins the race; those terminal
	// states prevail. Forward-progress controllers must treat this state the same as terminal (i.e. do not start
	// any new work on the request).
	RequestStateCancelling RequestState = "cancelling"
	// RequestStateCancelled is the state of a land request that was cancelled by the user before it could land. This is the final state.
	RequestStateCancelled RequestState = "cancelled"
)

// IsRequestStateTerminal returns true if the state represents a final, irreversible state (landed, error, or cancelled).
// RequestStateCancelling is intentionally excluded: cancellation is best-effort and a Cancelling request may still
// transition to Landed or Error before it reaches Cancelled. Callers that want to gate forward progress (and treat
// Cancelling as halted) should use IsRequestStateHalted instead.
func IsRequestStateTerminal(s RequestState) bool {
	return s == RequestStateLanded || s == RequestStateError || s == RequestStateCancelled
}

// IsRequestStateHalted returns true if the request is either terminal or in the process of being cancelled.
// Forward-progress controllers (validate, batch, ...) use this to short-circuit work for requests that the
// user has asked to cancel — even though Cancelling is non-terminal, no further pipeline work should start.
func IsRequestStateHalted(s RequestState) bool {
	return IsRequestStateTerminal(s) || s == RequestStateCancelling
}

// Request defines a request to land (merge into target branch of the source control repository) a set of code changes.
// The object is immutable after creation.
type Request struct {
	// ****************
	// Immutable fields, fixed at request entity creation
	// ****************

	// ID is the globally unique identifier for the land request. Format: "<queue>/<counter_value>".
	ID string `json:"id"`
	// Queue is the name of the queue processing the land request. Queue name is defined in the configuration and should be unique within the system.
	Queue string `json:"queue"`
	// Change is a number of code changes (such as pull requests) to land into the target branch. Target branch is defined by the queue configuration.
	Change change.Change `json:"change"`
	// LandStrategy is the source control integration strategy to use for this land operation.
	LandStrategy mergestrategy.MergeStrategy `json:"land_strategy"`

	// ****************
	// Following fields could be changed throughout the lifecycle of the request
	// ****************

	// State is the current state of the land request.
	State RequestState `json:"state"`
	// Version is the version of the object. It is used for optimistic locking.
	// Versioning starts at 1 and is incremented for each change to the object.
	Version int32 `json:"version"`
}

// ToBytes serializes the Request to JSON bytes for queue message payload.
func (r Request) ToBytes() ([]byte, error) {
	return json.Marshal(r)
}

// RequestFromBytes deserializes a Request from JSON bytes.
func RequestFromBytes(data []byte) (Request, error) {
	var req Request
	err := json.Unmarshal(data, &req)
	return req, err
}

// RequestID is a lightweight entity for publishing and consuming just the request identifier via the queue.
type RequestID struct {
	// ID is the globally unique identifier for the land request.
	ID string `json:"id"`
}

// ToBytes serializes the RequestID to JSON bytes for queue message payload.
func (r RequestID) ToBytes() ([]byte, error) {
	return json.Marshal(r)
}

// RequestIDFromBytes deserializes a RequestID from JSON bytes.
func RequestIDFromBytes(data []byte) (RequestID, error) {
	var rid RequestID
	err := json.Unmarshal(data, &rid)
	return rid, err
}
