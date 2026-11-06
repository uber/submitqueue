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

// RequestIssueKind classifies an accepted issue update within one request.
type RequestIssueKind string

const (
	RequestIssueKindNew      RequestIssueKind = "new"
	RequestIssueKindExisting RequestIssueKind = "existing"
	RequestIssueKindClosed   RequestIssueKind = "closed"
)

// RequestIssueSummary is a materialized result of one request's issue reconciliation.
// Its identity is request, task, and unit within a queue.
type RequestIssueSummary struct {
	// RequestID identifies the request that processed the issue.
	RequestID string `json:"request_id"`
	// TaskID identifies the logical validation task.
	TaskID string `json:"task_id"`
	// UnitID identifies the validation unit; empty for a task-wide failure.
	UnitID string `json:"unit_id"`
	// Kind distinguishes newly opened, already open, and closed issues.
	Kind RequestIssueKind `json:"kind"`
	// BuildID identifies the build whose accepted update produced this result.
	BuildID string `json:"build_id"`
	// FirstSeenBuildID identifies the opening, including for an existing or closed issue.
	FirstSeenBuildID string `json:"first_seen_build_id"`
	// Version is the optimistic-lock version, initially zero.
	Version int32 `json:"version"`
}

// Validate checks the identity and classification of a request issue.
func (i RequestIssueSummary) Validate() error {
	if i.Version < 0 {
		return fmt.Errorf("request issue summary version must not be negative")
	}
	if i.RequestID == "" || i.TaskID == "" || i.BuildID == "" || i.FirstSeenBuildID == "" {
		return fmt.Errorf("request issue requires request, task, build, and first-seen build IDs")
	}
	switch i.Kind {
	case RequestIssueKindNew, RequestIssueKindExisting, RequestIssueKindClosed:
		return nil
	default:
		return fmt.Errorf("invalid request issue kind %q", i.Kind)
	}
}
