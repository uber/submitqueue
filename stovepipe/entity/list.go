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

// ListRequest selects current request summaries from one queue by acceptance time.
type ListRequest struct {
	// Queue is the required configured queue containing the requests.
	Queue string
	// AcceptedAtOrAfterMs is the inclusive Unix millisecond bound when HasAcceptedAtOrAfterMs is true.
	AcceptedAtOrAfterMs int64
	// HasAcceptedAtOrAfterMs distinguishes an explicit lower bound from an omitted one.
	HasAcceptedAtOrAfterMs bool
	// AcceptedBeforeMs is the exclusive Unix millisecond bound when HasAcceptedBeforeMs is true.
	AcceptedBeforeMs int64
	// HasAcceptedBeforeMs distinguishes an explicit upper bound from an omitted one.
	HasAcceptedBeforeMs bool
	// PageSize is the maximum number of summaries, from 1 to 200; zero selects 50.
	PageSize int32
	// PageToken is an opaque continuation bound to the queue and resolved time window.
	PageToken string
}

// ListResult is one page of current summaries, not a snapshot across pages.
type ListResult struct {
	// Requests contains summaries ordered by descending acceptance time, then descending bytewise request ID.
	Requests []RequestSummary
	// NextPageToken continues after the last returned key; empty means no further mapping was observed.
	NextPageToken string
}
