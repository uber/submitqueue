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
)

// RequestState defines the internal state of a Stovepipe validation request as it moves
// through the pipeline. States are internal and used to implement a state machine; a
// customer-facing status type may be layered on top later, as in SubmitQueue.
type RequestState string

const (
	// RequestStateUnknown is the unreachable zero value. It is set by default when the
	// structure is initialized and should never be seen in the system.
	RequestStateUnknown RequestState = ""
	// RequestStateAccepted is the initial state of a request: a new commit has been observed
	// for the queue and the request has been admitted into the pipeline, but no validation
	// strategy has been chosen yet.
	RequestStateAccepted RequestState = "accepted"
	// RequestStateProcessing means process admitted the request, recorded build strategy and
	// baseline, and published to build.
	RequestStateProcessing RequestState = "processing"
	// RequestStateSuperseded means process skipped the request because a newer head exists.
	RequestStateSuperseded RequestState = "superseded"
)

// BuildStrategy defines how build validates the request's commit.
type BuildStrategy string

const (
	// BuildStrategyUnknown is the zero value before process chooses a strategy.
	BuildStrategyUnknown BuildStrategy = ""
	// BuildStrategyIncrementalSinceGreen validates only the delta since the pinned baseline URI.
	BuildStrategyIncrementalSinceGreen BuildStrategy = "incremental_since_green"
	// BuildStrategyFull validates the whole repo from scratch.
	BuildStrategyFull BuildStrategy = "full"
)

// Request represents a single validation of a queue at a particular commit. The queue reports
// a newly observed commit, Stovepipe mints a Request (identity namespaced by the queue), and
// the request flows through the pipeline accumulating state.
type Request struct {
	// ****************
	// Immutable fields, fixed at request entity creation
	// ****************

	// ID is the globally unique identifier for the request. Format: "request/<queue>/<counter>"
	// (e.g. "request/monorepo/main/42").
	ID string `json:"id"`
	// Queue is the name of the queue (a named repo+ref) being validated. It namespaces the ID
	// and is the stable handle the ingest caller supplies.
	Queue string `json:"queue"`
	// URI is the opaque, VCS-agnostic locator of the commit under validation, as produced by the
	// SourceControl extension.
	URI string `json:"uri"`

	// ****************
	// Set once at process admit — empty until accepted→processing; never overwritten after
	// ****************

	// BuildStrategy is the validation scope process chose when admitting this request.
	BuildStrategy BuildStrategy `json:"build_strategy"`
	// BaseURI is the base URI to be used for this request when the strategy is incremental.
	// Empty for full builds and cold start.
	BaseURI string `json:"base_uri"`

	// ****************
	// Following fields could be changed throughout the lifecycle of the request
	// ****************

	// State is the current state of the request in the pipeline.
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
