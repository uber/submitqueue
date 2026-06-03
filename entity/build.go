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

// BuildStatus defines the possible states of a build. The set is
// intentionally narrow: every supported build provider must be able to map
// its native lifecycle into one of these values without leaking
// provider-specific stages.
type BuildStatus string

const (
	// BuildStatusUnknown is the unreachable zero value, set by default when
	// the structure is initialized. It should never be seen in the system.
	BuildStatusUnknown BuildStatus = ""

	// BuildStatusAccepted indicates the build has been accepted for
	// execution.
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
// (Succeeded, Failed, or Cancelled).
func (s BuildStatus) IsTerminal() bool {
	return s == BuildStatusSucceeded || s == BuildStatusFailed || s == BuildStatusCancelled
}

// SpeculationPathInfo represents the base and head commits of a speculation path used in a build.
type SpeculationPathInfo struct {
	// Base is a list of batchIDs(in order) that form the base of this speculation path.
	Base []string
}

// Build represents a build scheduled for a batch along a specific speculation path.
// All fields except the Status are immutable after creation.
type Build struct {
	// ID represents the build ID. It is the responsibility of a build management system to ensure
	// that this is unique.
	ID string
	// BatchID is the batch for which this build is scheduled.
	BatchID string
	// SpeculationPath is the speculation path that represents this build. For
	// a given batch this path is crafted from the graph that is generated from the
	// dependencies of this batch.
	SpeculationPath SpeculationPathInfo
	// Score represents the build prediction score for this speculation path.
	Score float32
	// Status represents the state of the build lifecycle this build is in.
	Status BuildStatus
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

// BuildID is a lightweight entity for publishing and consuming just the build identifier via the queue.
type BuildID struct {
	// ID is the globally unique identifier for the build.
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

// BuildMetadata carries provider-defined free-form metadata about a build
// (e.g. build URL, duration, commit SHA). Keys and values are
// implementation-defined; callers should not assume any particular schema.
type BuildMetadata map[string]string
