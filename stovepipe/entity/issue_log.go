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

// IssueEvent identifies an issue lifecycle transition.
type IssueEvent string

const (
	// IssueEventUnknown is the unset event value.
	IssueEventUnknown IssueEvent = ""
	// IssueEventOpened records that an issue became open.
	IssueEventOpened IssueEvent = "opened"
	// IssueEventResolved records that an issue was resolved.
	IssueEventResolved IssueEvent = "resolved"
)

// IssueLog is an immutable issue lifecycle transition within a queue.
// Task, unit, build, and event identify one transition.
type IssueLog struct {
	// TaskID identifies the logical validation task.
	TaskID string `json:"task_id"`
	// UnitID identifies the validation unit; empty for a task-wide issue.
	UnitID string `json:"unit_id"`
	// Event identifies the opening or resolution.
	Event IssueEvent `json:"event"`
	// BuildID identifies the build where the issue was first seen or confirmed resolved.
	BuildID string `json:"build_id"`
	// TimestampMs is when the transition was first recorded, in Unix milliseconds.
	TimestampMs int64 `json:"timestamp_ms"`
}

// Validate verifies the invariants required for a newly persisted issue log.
func (l IssueLog) Validate() error {
	if l.BuildID == "" {
		return fmt.Errorf("issue log build ID must not be empty")
	}
	if l.TimestampMs <= 0 {
		return fmt.Errorf("issue log timestamp must be positive")
	}
	switch l.Event {
	case IssueEventOpened, IssueEventResolved:
		return nil
	default:
		return fmt.Errorf("invalid issue event %q", l.Event)
	}
}
