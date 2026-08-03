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

// Build represents a build scheduled for a batch along a specific speculation path.
// All fields except the Status are immutable after creation.
//
// It is keyed by the runner's build ID, which is the identifier every stage
// downstream of the trigger already holds: a poll, a webhook, and a runner-side
// log line all name a build, none of them names a speculation path. The path
// coordinates ride along on the record so those stages never have to
// understand speculation to do their job.
type Build struct {
	// ID is the identifier minted by the queue's build runner when the build
	// is triggered; this is the primary storage key.
	ID string
	// BatchID is the batch for which this build is scheduled.
	BatchID string
	// PathID is the speculation path this build verifies, as carried by
	// SpeculationPathEntry.ID.
	PathID string
	// Attempt is which build attempt for that path this is, starting at 1.
	// A path may be built more than once, so ID names the run while
	// (PathID, Attempt) names the slot it occupies.
	Attempt int
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
	// Queue is the name of the queue processing the batch this build verifies. Empty on payloads written before the field existed.
	Queue string `json:"queue"`
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
