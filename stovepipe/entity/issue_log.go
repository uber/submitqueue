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

// IssueEvent identifies an issue occurrence; reporting does not change status.
type IssueEvent string

const (
	IssueEventOpened   IssueEvent = "open"
	IssueEventReported IssueEvent = "reported"
	IssueEventClosed   IssueEvent = "closed"
)

// IssueLog is an immutable occurrence identified by task, unit, build, and event within a queue.
type IssueLog struct {
	// TaskID identifies the logical validation task.
	TaskID string `json:"task_id"`
	// UnitID identifies the validation unit; empty for a task-wide issue.
	UnitID string `json:"unit_id"`
	// Event identifies an opening, repeat report, or closure.
	Event IssueEvent `json:"event"`
	// BuildID identifies the build that produced this occurrence.
	BuildID string `json:"build_id"`
	// TimestampMs is when this occurrence was first recorded, in Unix milliseconds.
	TimestampMs int64 `json:"timestamp_ms"`
	// FirstSeenBuildID identifies this opening; empty only on legacy lifecycle records.
	FirstSeenBuildID string `json:"first_seen_build_id"`
}

// Validate checks the event identity, timestamp, and opening reference.
func (l IssueLog) Validate() error {
	if l.BuildID == "" {
		return fmt.Errorf("issue log build ID must not be empty")
	}
	if l.TimestampMs <= 0 {
		return fmt.Errorf("issue log timestamp must be positive")
	}
	switch l.Event {
	case IssueEventOpened:
		if l.FirstSeenBuildID != "" && l.FirstSeenBuildID != l.BuildID {
			return fmt.Errorf("issue opening must reference its source build")
		}
	case IssueEventReported:
		if l.FirstSeenBuildID == "" {
			return fmt.Errorf("reported issue requires an opening build")
		}
		if l.FirstSeenBuildID == l.BuildID {
			return fmt.Errorf("reported issue must reference an earlier opening")
		}
	case IssueEventClosed:
	default:
		return fmt.Errorf("invalid issue event %q", l.Event)
	}
	return nil
}
