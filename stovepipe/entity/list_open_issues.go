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

// ListOpenIssuesRequest selects current open issues within one queue.
type ListOpenIssuesRequest struct {
	// Queue is the required configured queue containing the issues.
	Queue string
	// PageSize bounds returned issues, from 1 to 200; zero selects 50.
	PageSize int32
	// PageToken is an opaque continuation bound to the queue and task/unit order.
	PageToken string
}

// ListOpenIssuesResult is a bounded page of current open issue summaries.
type ListOpenIssuesResult struct {
	// Issues contains matching open summaries in ascending bytewise task/unit order.
	Issues []IssueSummary
	// NextPageToken continues after the last examined open-issue row, including skipped entries.
	// Empty means no further open-issue row was observed, not that the queue is healthy.
	NextPageToken string
}
