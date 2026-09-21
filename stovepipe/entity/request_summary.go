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

// RequestSummary is the materialized current view of a validation request.
type RequestSummary struct {
	// RequestID is the globally unique request identifier.
	RequestID string
	// Queue is the queue containing the request.
	Queue string
	// URI is the commit validated by the request.
	URI string
	// BaseURI is the incremental-validation baseline and is empty before selection or for a full build.
	BaseURI string
	// State is the current durable request lifecycle state.
	State RequestState
	// RequestVersion is the version of the request state represented by this summary.
	RequestVersion int32
	// StateTimestampMs is when the represented state was first retained, in Unix milliseconds.
	StateTimestampMs int64
	// Version is the optimistic-lock version of this materialized view.
	Version int32
}
