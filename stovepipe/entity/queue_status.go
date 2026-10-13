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

// GetQueueStatusRequest selects a queue and optionally an exact commit in its history.
type GetQueueStatusRequest struct {
	// Queue is the stable repo-and-ref namespace.
	Queue string
	// ChangeURI is an optional exact commit URI, including commits without validation requests.
	ChangeURI string
}

// GetQueueStatusResult contains the enablement selected by the request.
type GetQueueStatusResult struct {
	// Queue is the selected repo-and-ref namespace.
	Queue string
	// State is the latest materialized enablement or the occurrence covering the requested commit.
	State QueueState
}
