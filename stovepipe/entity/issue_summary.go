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

	"github.com/uber/submitqueue/platform/base/id"
)

// IssueSummary is the latest read model of an issue within a queue.
// Closed summaries retain the source of their latest transition.
type IssueSummary struct {
	// TaskID identifies the logical validation task and must be nonempty.
	TaskID string
	// UnitID identifies the validation unit; empty means a task-wide issue.
	UnitID string
	// Status identifies whether the issue is open or closed.
	Status IssueStatus
	// FirstSeenBuildID identifies the current or most recent opening.
	FirstSeenBuildID string
	// RequestID is the canonical decimal source request ID of the latest transition.
	RequestID string
	// BuildID identifies the source build of the latest transition.
	BuildID string
	// ChangedAtMs is when the latest transition was first recorded, in Unix milliseconds.
	ChangedAtMs int64
	// Version is the positive optimistic-lock version of this summary.
	Version int32
}

// Validate checks the identity, status, timestamp, and version of a summary.
func (s IssueSummary) Validate() error {
	if s.TaskID == "" || s.FirstSeenBuildID == "" || s.BuildID == "" {
		return fmt.Errorf("issue summary requires task, first-seen build, and source build IDs")
	}
	if err := id.Validate(s.RequestID); err != nil {
		return fmt.Errorf("invalid issue summary request ID: %w", err)
	}
	if s.Status != IssueStatusOpen && s.Status != IssueStatusClosed {
		return fmt.Errorf("invalid issue summary status %q", s.Status)
	}
	if s.ChangedAtMs <= 0 {
		return fmt.Errorf("issue summary requires a positive change timestamp")
	}
	if s.Version <= 0 {
		return fmt.Errorf("issue summary requires a positive version")
	}
	return nil
}
