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

type GetQueueStatusRequest struct {
	// Queue is the stable repo-and-ref namespace.
	Queue string
	// ChangeURI is an immutable commit URI; empty selects current enablement.
	ChangeURI string
}

type GetQueueStatusResult struct {
	// Queue is the selected repo-and-ref namespace.
	Queue string
	// Enablement is current enablement or enablement covering the requested commit.
	Enablement QueueState
}
