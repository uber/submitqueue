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

// BuildStatus defines the possible states of a build. Shaped the same as
// SubmitQueue's own BuildStatus (submitqueue/entity/build.go), but defined
// locally rather than shared — see build.md's "Alternatives considered for
// sharing the contract".
type BuildStatus string

const (
	// BuildStatusUnknown is the unreachable zero value, set by default when
	// the structure is initialized. It should never be seen in the system.
	BuildStatusUnknown BuildStatus = ""

	// BuildStatusAccepted indicates the build has been accepted for
	// execution by the runner via Trigger, but has not yet started.
	BuildStatusAccepted BuildStatus = "accepted"

	// BuildStatusRunning indicates the build is currently executing.
	BuildStatusRunning BuildStatus = "running"

	// BuildStatusSucceeded indicates the build completed successfully.
	// This is a terminal state.
	BuildStatusSucceeded BuildStatus = "succeeded"

	// BuildStatusFailed indicates the build did not complete successfully.
	// This is a terminal state.
	BuildStatusFailed BuildStatus = "failed"

	// BuildStatusCancelled indicates the build was cancelled.
	// This is a terminal state.
	BuildStatusCancelled BuildStatus = "cancelled"
)

// IsTerminal returns true if the status represents a final state
// (Succeeded, Failed, or Cancelled). A terminal status is write-once: once
// buildsignal persists one, a later poll reporting a different terminal
// value must never overwrite it (see buildsignal.md's Algorithm, step 6).
func (s BuildStatus) IsTerminal() bool {
	return s == BuildStatusSucceeded || s == BuildStatusFailed || s == BuildStatusCancelled
}

// Build represents a single build triggered for a Request's commit. All
// fields except Status and Version are immutable after creation — build is
// the sole creator (via BuildStore.Create), and buildsignal is the sole
// writer of Status/Version afterward.
type Build struct {
	// ID is the build's own key: the runner-assigned id returned by
	// Trigger (e.g. a Buildkite build number). Opaque; never parsed or
	// derived by stovepipe.
	ID string `json:"id"`
	// RequestID is the Request this build validates (Build->Request
	// navigation; no reverse index from Request to its builds is needed).
	RequestID string `json:"request_id"`
	// URI is the head URI being built (== Request.URI).
	URI string `json:"uri"`
	// BaseURI is the incremental baseline; empty for full builds.
	BaseURI string `json:"base_uri"`
	// Status is the build's lifecycle state.
	Status BuildStatus `json:"status"`
	// Version is used for optimistic locking. Versioning starts at 1 and
	// is incremented for each change to the object.
	Version int32 `json:"version"`
}

// ToBytes serializes the Build to JSON bytes for queue message payload.
func (b Build) ToBytes() ([]byte, error) {
	return json.Marshal(b)
}

// BuildFromBytes deserializes a Build from JSON bytes.
func BuildFromBytes(data []byte) (Build, error) {
	var build Build
	err := json.Unmarshal(data, &build)
	return build, err
}

// BuildID is a lightweight entity for publishing and consuming just the
// build identifier via the queue, and for the BuildRunner Status/Cancel
// parameter. It wraps the one runner-assigned id everywhere it appears.
type BuildID struct {
	// ID is the runner-assigned identifier for the build.
	ID string `json:"id"`
}

// ToBytes serializes the BuildID to JSON bytes for queue message payload.
func (b BuildID) ToBytes() ([]byte, error) {
	return json.Marshal(b)
}

// BuildIDFromBytes deserializes a BuildID from JSON bytes.
func BuildIDFromBytes(data []byte) (BuildID, error) {
	var bid BuildID
	err := json.Unmarshal(data, &bid)
	return bid, err
}

// BuildMetadata carries caller-supplied, provider-echoed free-form metadata
// about a build. The runner must not depend on its contents. Empty today;
// expected to carry real data eventually (e.g. conflict-graph info, or other
// upstream decisions relevant to the build) once a concrete need lands in
// either domain — the shape is deferred until then, not decided here.
type BuildMetadata map[string]string
