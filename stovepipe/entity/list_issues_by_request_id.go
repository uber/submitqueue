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

// ListIssuesByRequestIDRequest selects observations across one request's retained builds.
type ListIssuesByRequestIDRequest struct {
	// Queue is the configured queue containing the request.
	Queue string
	// RequestID is the canonical decimal request identifier.
	RequestID string
	// PageSize bounds returned observations; zero selects 50, otherwise 1 through 200.
	PageSize int32
	// PageToken is an opaque continuation bound to the queue and request.
	PageToken string
}

// RequestIssue is a build observation independent of current issue status.
type RequestIssue struct {
	// BuildID identifies the source build.
	BuildID string
	// TaskID identifies the validation task.
	TaskID string
	// UnitID identifies the validation unit; empty means task-wide.
	UnitID string
	// FixID is the opaque repair association; empty means none is recorded.
	FixID string
}

// ListIssuesByRequestIDResult is a live page of observations with build provenance.
type ListIssuesByRequestIDResult struct {
	// Queue contains the selected request.
	Queue string
	// RequestID identifies the selected request within its queue.
	RequestID string
	// Issues contains observations in bytewise build/task/unit order.
	Issues []RequestIssue
	// NextPageToken continues the traversal; empty means exhausted, not complete analysis.
	NextPageToken string
}
