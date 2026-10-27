// Copyright (c) 2026 Uber Technologies, Inc.
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

import "fmt"

// IssueStatus describes whether a validation issue is open or closed.
type IssueStatus string

const (
	// IssueStatusUnknown is the unset status.
	IssueStatusUnknown IssueStatus = ""
	// IssueStatusOpen identifies an open validation issue.
	IssueStatusOpen IssueStatus = "open"
	// IssueStatusClosed identifies an issue that is no longer open.
	IssueStatusClosed IssueStatus = "closed"
)

// IssueState is the latest state of a validation condition within a queue.
// Closed records retain their identity and version across later openings.
type IssueState struct {
	// TaskID identifies the logical validation task and must be nonempty.
	TaskID string `json:"task_id"`
	// UnitID identifies the validation unit; empty for a task-wide issue.
	UnitID string `json:"unit_id"`
	// Status distinguishes open and closed issues.
	Status IssueStatus `json:"status"`
	// FirstSeenBuildID identifies the current or most recent opening.
	FirstSeenBuildID string `json:"first_seen_build_id"`
	// LastProcessedBuildID identifies the most recent build applied to this issue.
	LastProcessedBuildID string `json:"last_processed_build_id"`
	// LastChangedBuildID identifies the build responsible for the current open or closed status.
	LastChangedBuildID string `json:"last_changed_build_id"`
	// ChangedAtMs is the latest status transition's timestamp in Unix milliseconds.
	ChangedAtMs int64 `json:"changed_at_ms"`
	// Version is the positive optimistic-lock version; it never resets on reopening.
	Version int32 `json:"version"`
}

// Validate checks the identity, status, and version of an issue state.
func (s IssueState) Validate() error {
	if s.TaskID == "" || s.FirstSeenBuildID == "" {
		return fmt.Errorf("issue state requires task and first-seen build IDs")
	}
	if s.LastProcessedBuildID == "" || s.LastChangedBuildID == "" {
		return fmt.Errorf("issue state requires last-processed and last-changed build IDs")
	}
	if s.ChangedAtMs <= 0 {
		return fmt.Errorf("issue state requires a positive change timestamp")
	}
	if s.Status != IssueStatusOpen && s.Status != IssueStatusClosed {
		return fmt.Errorf("invalid issue status %q", s.Status)
	}
	if s.Version <= 0 {
		return fmt.Errorf("issue state version must be positive")
	}
	return nil
}
