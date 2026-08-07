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

// GetRequestHistoryByIDRequest identifies one retained request history by request ID.
type GetRequestHistoryByIDRequest struct {
	// ID is the globally unique identifier of the request.
	ID string
	// Queue is the name of the queue processing the request. It scopes the lookup:
	// a request is only resolvable within its own queue.
	Queue string
}

// GetRequestHistoryByChangeURIRequest identifies retained histories by an exact pinned change URI.
type GetRequestHistoryByChangeURIRequest struct {
	// ChangeURI is the exact change URI supplied in a Land request.
	ChangeURI string
	// Queue is the name of the queue to search. It scopes the lookup: a change URI
	// landed into several queues matches separately in each.
	Queue string
}

// RequestHistory groups retained events for one request.
type RequestHistory struct {
	// RequestID is the globally unique identifier of the request.
	RequestID string
	// Events are retained request-log events ordered chronologically.
	Events []RequestLog
}
