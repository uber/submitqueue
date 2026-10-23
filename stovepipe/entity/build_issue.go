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

// BuildIssue identifies a validation failure by task and optional unit.
type BuildIssue struct {
	// TaskID identifies the logical validation task, stable across builds and retries; it must be nonempty.
	TaskID string `json:"task_id"`
	// UnitID identifies the failed unit within the task; empty for a task-wide issue.
	// Repeated observations of the same condition use the same TaskID and UnitID.
	UnitID string `json:"unit_id"`
}
