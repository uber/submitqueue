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

// CommitStatus is the validation state of a trunk commit as determined by Stovepipe.
type CommitStatus string

const (
	// CommitStatusUnknown is the default state when a commit is first ingested.
	// The commit has landed on main but has not yet been validated.
	CommitStatusUnknown CommitStatus = ""
	// CommitStatusSucceeded means the relevant targets build and test successfully at this commit.
	CommitStatusSucceeded CommitStatus = "succeeded"
	// CommitStatusFailed means a target is broken at this commit; it is the offending change.
	CommitStatusFailed CommitStatus = "failed"
)

// IsCommitStatusTerminal returns true if the status is a final, irreversible state.
func IsCommitStatusTerminal(s CommitStatus) bool {
	return s == CommitStatusSucceeded || s == CommitStatusFailed
}

// Commit is a trunk commit tracked by Stovepipe. The SHA scoped by Repository and
// Branch is the natural identity and dedup key: a commit announced by both a webhook
// and a poll backfill resolves to the same record and is processed once.
type Commit struct {
	// SHA is the full commit hash. Identity key; immutable after creation.
	SHA string
	// Repository is the repository URI (e.g. "github.com/uber/go-code").
	Repository string
	// Branch is the target branch (e.g. "main").
	Branch string
	// CommitterTimeMs is the committer timestamp in milliseconds since epoch.
	// Used to order commits within a range and to establish the trunk sequence.
	CommitterTimeMs int64
	// Status is the current validation state of this commit.
	Status CommitStatus
	// Version is incremented on each update and used for optimistic locking.
	// Version arithmetic lives in the controller; the store performs a pure conditional write.
	Version int32
	// CreatedAt is the time this commit was first recorded, in milliseconds since epoch.
	CreatedAt int64
	// UpdatedAt is the time this commit was last updated, in milliseconds since epoch.
	UpdatedAt int64
}
