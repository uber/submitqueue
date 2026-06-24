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

	"github.com/uber/submitqueue/platform/base/change"
)

// RequestState defines the possible states of a Stovepipe trunk-validation request. They are internal
// and used to implement a state machine. A separate RequestStatus type tracks the customer-friendly
// status of a request. Stovepipe validates commits after they land, so the terminal states describe a
// commit's health (succeeded / failed) rather than a merge outcome.
type RequestState string

const (
	// RequestStateUnknown is the unreachable sentinel state. It is set by default when the structure is
	// initialized and should never be seen in the system.
	RequestStateUnknown RequestState = ""
	// RequestStateStarted is the initial state of a request. The commit has been recorded by the
	// orchestrator and is entering the pipeline, but no validation has happened yet.
	RequestStateStarted RequestState = "started"
	// RequestStateValidated indicates that the commit metadata needed for ordering and batching has been
	// resolved successfully.
	RequestStateValidated RequestState = "validated"
	// RequestStateBatched indicates that the request has been enrolled in a validation batch (a contiguous
	// range of commits since the last known green).
	RequestStateBatched RequestState = "batched"
	// RequestStateBuilding indicates that the batch containing the request is being built and tested.
	RequestStateBuilding RequestState = "building"
	// RequestStateSucceeded is the terminal state of a request whose commit validated green. This is a
	// final state.
	RequestStateSucceeded RequestState = "succeeded"
	// RequestStateFailed is the terminal state of a request whose commit was found to break a target. This
	// is a final state.
	RequestStateFailed RequestState = "failed"
	// RequestStateError is the terminal state of a request that encountered an infrastructure error and
	// could not be validated. This is a final state.
	RequestStateError RequestState = "error"
)

// IsRequestStateTerminal returns true if the state represents a final, irreversible state (succeeded,
// failed, or error). Forward-progress controllers use this to short-circuit work for requests that have
// already reached a terminal outcome.
func IsRequestStateTerminal(s RequestState) bool {
	return s == RequestStateSucceeded || s == RequestStateFailed || s == RequestStateError
}

// Request defines a Stovepipe request to validate a set of trunk commits after they have landed on the
// target branch. The immutable fields are fixed at creation; the mutable fields advance as the request
// moves through the pipeline.
type Request struct {
	// ****************
	// Immutable fields, fixed at request entity creation
	// ****************

	// ID is the globally unique identifier for the request (the "spid"). Format: "<queue>/<counter_value>".
	ID string `json:"id"`
	// Queue is the name of the queue processing the request. Queue name is defined in the configuration and
	// should be unique within the system.
	Queue string `json:"queue"`
	// Change is the set of trunk commits to validate, identified by URI. The scheme names the VCS; the rest
	// is provider-specific (e.g. git://remote/repo/ref/commit_sha).
	Change change.Change `json:"change"`

	// ****************
	// Following fields could be changed throughout the lifecycle of the request
	// ****************

	// State is the current state of the request.
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

// RequestID is a lightweight entity for publishing and consuming just the request identifier via the
// queue. Internal pipeline hops carry the ID and reload the full request from storage.
type RequestID struct {
	// ID is the globally unique identifier for the request.
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
