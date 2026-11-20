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

// ListIssueHistoryRequest selects retained occurrences of one queue-scoped issue.
type ListIssueHistoryRequest struct {
	// Queue identifies the configured queue.
	Queue string
	// TaskID identifies the validation task.
	TaskID string
	// UnitID identifies the validation unit; empty means task-wide.
	UnitID string
	// PageSize is the maximum number of events; zero selects the default.
	PageSize int32
	// PageToken is the opaque continuation bound to this issue.
	PageToken string
}

// IssueHistoryEntry combines an immutable occurrence with its source request.
type IssueHistoryEntry struct {
	// Log is the retained issue occurrence.
	Log IssueLog
	// RequestID is the source request; empty when the build is no longer retained.
	RequestID string
}

// ListIssueHistoryResult is one page of retained occurrences, not complete coverage.
type ListIssueHistoryResult struct {
	// Events are ordered by recorded timestamp, build ID, and event ascending.
	Events []IssueHistoryEntry
	// NextPageToken continues the scan; empty ends the currently observed range.
	NextPageToken string
}
