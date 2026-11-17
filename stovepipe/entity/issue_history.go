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

// ListIssueHistoryRequest selects retained issue occurrences within a queue.
type ListIssueHistoryRequest struct {
	// Queue identifies the configured queue.
	Queue string
	// TaskID optionally selects one issue; empty includes all issues and requires an empty UnitID.
	TaskID string
	// UnitID selects the exact unit within TaskID; empty means that task's task-wide issue.
	UnitID string
	// PageSize is the maximum number of events; zero selects the default.
	PageSize int32
	// PageToken is the opaque continuation bound to this issue filter and request window.
	PageToken string
	// AcceptedAtOrAfterMs is the inclusive request-acceptance bound in Unix milliseconds when supplied.
	AcceptedAtOrAfterMs int64
	// HasAcceptedAtOrAfterMs distinguishes an explicit lower bound from the default zero.
	HasAcceptedAtOrAfterMs bool
	// AcceptedBeforeMs is the exclusive request-acceptance bound in Unix milliseconds when supplied.
	AcceptedBeforeMs int64
	// HasAcceptedBeforeMs distinguishes an explicit upper bound from server now on the first page.
	HasAcceptedBeforeMs bool
}

// IssueHistoryEntry combines an immutable occurrence with its source request.
type IssueHistoryEntry struct {
	// Log is the retained issue occurrence.
	Log IssueLog
	// RequestID identifies the accepted request whose issue projection references this build.
	RequestID string
}

// ListIssueHistoryResult is one page of retained occurrences, not complete coverage.
type ListIssueHistoryResult struct {
	// Events follow descending request-acceptance order, then ascending bytewise task/unit order.
	Events []IssueHistoryEntry
	// NextPageToken continues after the last examined request/issue, including skipped projections or logs.
	// Pages can be empty with a continuation; an empty token ends the observed request window.
	NextPageToken string
	// AcceptedAtOrAfterMs is the inclusive request-acceptance window bound in Unix milliseconds.
	AcceptedAtOrAfterMs int64
	// AcceptedBeforeMs is the exclusive request-acceptance window bound in Unix milliseconds.
	AcceptedBeforeMs int64
}
