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

// OpenIssue is a currently open validation issue within a queue.
// Its task and unit identify the condition; the first-seen build identifies
// this opening and is immutable until removal.
type OpenIssue struct {
	// TaskID identifies the logical validation task.
	TaskID string `json:"task_id"`
	// UnitID identifies the validation unit; empty for a task-wide issue.
	UnitID string `json:"unit_id"`
	// FirstSeenBuildID identifies the build where this opening of the issue was first observed.
	FirstSeenBuildID string `json:"first_seen_build_id"`
}
