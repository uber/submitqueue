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

// QueuePause is a point-in-time snapshot of the pause switches of a single
// queue. The two switches are independent, and the zero value means the queue
// is not paused.
type QueuePause struct {
	// RejectLands is true while the queue refuses new land requests.
	RejectLands bool

	// FreezeProcessing is true while the queue's accepted requests are not advanced.
	FreezeProcessing bool

	// Reason is the operator's free-text explanation for the pause. Empty when none was given.
	Reason string
}
