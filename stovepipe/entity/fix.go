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

// FixState is a repair's submission state, independent of execution or check health.
type FixState string

const (
	FixStateUnknown FixState = ""
	FixQueued       FixState = "queued"
	// FixSubmitting means a dispatch claim is durable; acceptance is unknown.
	FixSubmitting FixState = "submitting"
	// FixSubmitted means a provider returned an execution reference.
	FixSubmitted FixState = "submitted"
	// FixSubmissionUnknown means the trigger returned an error or no reference.
	FixSubmissionUnknown FixState = "submission_unknown"
	FixSkipped           FixState = "skipped"
)

// Fix is a versioned repair submission associated with issue openings.
type Fix struct {
	// ID is the queue-scoped attempt identifier, stable across message redelivery.
	ID string `json:"id"`
	// Queue is the owning validation queue.
	Queue string `json:"queue"`
	// BuildID is the build whose confirmed failure initiated this attempt.
	BuildID string `json:"build_id"`
	// State is the submission state; submitted does not imply execution or recovery.
	State FixState `json:"state"`
	// Issues contains issue-opening references; the initial submission contains one issue.
	Issues []OpenIssue `json:"issues"`
	// ExecutionRef is the optional provider reference returned by submission, empty until known.
	ExecutionRef string `json:"execution_ref"`
	// Metadata contains optional change references and provider context; nil and empty maps are equivalent.
	Metadata map[string]string `json:"metadata"`
	// Version is the optimistic storage version.
	Version int32 `json:"version"`
	// CreatedAt is the UTC creation time in milliseconds since the epoch.
	CreatedAt int64 `json:"created_at"`
	// UpdatedAt is the UTC update time in milliseconds since the epoch.
	UpdatedAt int64 `json:"updated_at"`
}
