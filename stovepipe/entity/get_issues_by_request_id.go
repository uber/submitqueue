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

// RequestIssuesStatus describes coverage of a request's retained issue results.
type RequestIssuesStatus string

const (
	RequestIssuesStatusUnavailable RequestIssuesStatus = "unavailable"
	RequestIssuesStatusInProgress  RequestIssuesStatus = "in_progress"
	RequestIssuesStatusComplete    RequestIssuesStatus = "complete"
	RequestIssuesStatusIncomplete  RequestIssuesStatus = "incomplete"
)

// GetIssuesByRequestIDRequest selects accepted issue results for one request.
type GetIssuesByRequestIDRequest struct {
	// Queue is the required configured queue containing the request.
	Queue string
	// RequestID is the canonical positive decimal request ID within the queue.
	RequestID string
	// PageSize bounds returned issues, from 1 to 200; zero selects 50.
	PageSize int32
	// PageToken is an opaque continuation bound to this queue and request.
	PageToken string
}

// GetIssuesByRequestIDResult is a page of request results, not all issues left open.
type GetIssuesByRequestIDResult struct {
	// Issues contains accepted results in ascending bytewise task/unit order.
	Issues []RequestIssueSummary
	// Status is complete only after reconciliation and every result write finished.
	// Unavailable means recording has not started or predates retained coverage.
	Status RequestIssuesStatus
	// NextPageToken continues after the last returned result; empty ends this scan.
	// In-progress results can gain entries behind the cursor; restart after completion.
	NextPageToken string
}
