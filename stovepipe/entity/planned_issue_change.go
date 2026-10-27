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

import (
	"fmt"
	"math"
)

// PlannedIssueChange describes an intended issue transition and its original version guard.
// The zero value means no change is planned.
type PlannedIssueChange struct {
	// Status is the intended open or closed status.
	Status IssueStatus `json:"status"`
	// FirstSeenBuildID identifies the opening retained by the intended issue state.
	FirstSeenBuildID string `json:"first_seen_build_id"`
	// ExpectedVersion is the original issue version; zero requires absence.
	ExpectedVersion int32 `json:"expected_version"`
	// TimestampMs is the transition's retained history timestamp in Unix milliseconds.
	TimestampMs int64 `json:"timestamp_ms"`
}

// Validate checks a complete change or the empty sentinel.
func (c PlannedIssueChange) Validate() error {
	if c == (PlannedIssueChange{}) {
		return nil
	}
	if c.ExpectedVersion < 0 || c.ExpectedVersion == math.MaxInt32 {
		return fmt.Errorf("planned issue version must permit a positive successor")
	}
	if c.Status != IssueStatusOpen && c.Status != IssueStatusClosed {
		return fmt.Errorf("invalid planned issue status %q", c.Status)
	}
	if c.FirstSeenBuildID == "" {
		return fmt.Errorf("planned issue change requires a first-seen build ID")
	}
	if c.Status == IssueStatusClosed && c.ExpectedVersion == 0 {
		return fmt.Errorf("closing an issue requires an existing version")
	}
	if c.TimestampMs <= 0 {
		return fmt.Errorf("planned issue timestamp must be positive")
	}
	return nil
}
