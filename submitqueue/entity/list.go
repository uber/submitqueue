// Copyright (c) 2025 Uber Technologies, Inc.
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

// ListRequest defines one bounded queue receipt-history query.
type ListRequest struct {
	// Queue is the exact queue to query.
	Queue string
	// ReceivedAtOrAfterMs is the inclusive lower receipt-time bound in Unix milliseconds.
	ReceivedAtOrAfterMs int64
	// ReceivedBeforeMs is the exclusive upper receipt-time bound in Unix milliseconds.
	ReceivedBeforeMs int64
	// PageSize is the maximum number of results to return. Zero selects the server default.
	PageSize int32
	// PageToken is an opaque continuation token from a previous result.
	PageToken string
}

// ListResult contains one page of queue receipt history.
type ListResult struct {
	// Requests are ordered by receipt time descending, then request ID descending.
	Requests []RequestQueueSummary
	// NextPageToken is an opaque continuation token. Empty means this is the last page.
	NextPageToken string
}
