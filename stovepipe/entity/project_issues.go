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

type ListRequestProjectsByIssueRequest struct {
	// Queue identifies the configured validation queue.
	Queue string
	// RequestID is the queue-scoped canonical decimal request ID.
	RequestID string
	// TaskID identifies the validation task.
	TaskID string
	// UnitID identifies the failed unit and must be nonempty.
	UnitID string
	// PageSize bounds returned associations; zero selects 50, otherwise 1 through 200.
	PageSize int32
	// PageToken continues the same request and issue lookup.
	PageToken string
}

type ListRequestIssuesByProjectRequest struct {
	// Queue identifies the configured validation queue.
	Queue string
	// RequestID is the queue-scoped canonical decimal request ID.
	RequestID string
	// Project identifies the affected project.
	Project string
	// PageSize bounds returned associations; zero selects 50, otherwise 1 through 200.
	PageSize int32
	// PageToken continues the same request and project lookup.
	PageToken string
}

type ListRequestProjectIssuesResult struct {
	// Attributions contains immutable pairs, ordered by project for an issue lookup
	// or by task/unit for a project lookup. Empty until attribution is complete.
	Attributions []ProjectIssueAttribution
	// AttributionComplete is true only after all request associations were recorded.
	// False does not establish that the issue or project has no associations.
	AttributionComplete bool
	// NextPageToken continues after the last returned pair; empty ends the range.
	NextPageToken string
}
