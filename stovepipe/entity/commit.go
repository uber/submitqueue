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

// CommitStatusKind is the validation state of a trunk commit as determined by Stovepipe.
type CommitStatusKind string

const (
	// CommitStatusKindUnknown is the default state when a commit is first ingested.
	// The commit has landed on main but has not yet been validated.
	CommitStatusKindUnknown CommitStatusKind = ""
	// CommitStatusKindIngested means the commit has been received and recorded by the gateway.
	CommitStatusKindIngested CommitStatusKind = "ingested"
	// CommitStatusKindQueued means the commit is waiting to enter the validation pipeline.
	CommitStatusKindQueued CommitStatusKind = "queued"
	// CommitStatusKindProcessing means the commit is actively being validated.
	CommitStatusKindProcessing CommitStatusKind = "processing"
	// CommitStatusKindSucceeded means the relevant targets build and test successfully at this commit.
	CommitStatusKindSucceeded CommitStatusKind = "succeeded"
	// CommitStatusKindFailed means a target is broken at this commit; it is the offending change.
	CommitStatusKindFailed CommitStatusKind = "failed"
)

// IsCommitStatusTerminal returns true if the status is a final, irreversible state.
func IsCommitStatusTerminal(s CommitStatusKind) bool {
	return s == CommitStatusKindSucceeded || s == CommitStatusKindFailed
}

// Commit is a trunk commit tracked by Stovepipe's gateway.
// URI is the primary key — it is the canonical change identity from the originating ChangeEvent.
type Commit struct {
	// URI is the canonical change identity from the originating ChangeEvent.
	URI string
	// SequenceNumber is the number of commits reachable from this commit on the trunk branch,
	// derived from `git rev-list --count`. Higher values are newer.
	// Must be populated at ingestion time — a zero value indicates the field was not set.
	SequenceNumber int64
	// CreatedAt is the time this commit was first recorded, in milliseconds since epoch.
	CreatedAt int64
}

// CommitStatus is a point-in-time validation status entry for a Commit.
// Multiple CommitStatus records form the status history of a single Commit.
type CommitStatus struct {
	// CommitURI is the URI of the Commit this status belongs to.
	CommitURI string
	// Status is the validation state recorded at this point in time.
	Status CommitStatusKind
	// CreatedAt is the time this status was recorded, in milliseconds since epoch.
	CreatedAt int64
}
