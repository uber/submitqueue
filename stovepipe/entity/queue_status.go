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

// QueueExecutionState describes the current queue-level pause control, not service health.
type QueueExecutionState string

const (
	// QueueExecutionStateUnknown means the pause control could not be determined.
	QueueExecutionStateUnknown QueueExecutionState = ""
	// QueueExecutionStateRunning means the queue-level control is unpaused.
	QueueExecutionStateRunning QueueExecutionState = "running"
	// QueueExecutionStatePaused means the queue-level control is paused.
	QueueExecutionStatePaused QueueExecutionState = "paused"
)

// QueueExecutionStatus is an observation of the current queue-level pause control.
type QueueExecutionStatus struct {
	// State is independent of policy and does not imply progress or a validation verdict.
	State QueueExecutionState
	// ObservedAtMs is the observation timestamp in Unix milliseconds, not a pause transition time.
	ObservedAtMs int64
}

// GetQueueStatusRequest selects a queue and optionally an exact commit in its history.
type GetQueueStatusRequest struct {
	// Queue is the stable repo-and-ref namespace.
	Queue string
	// ChangeURI is an optional exact commit URI, including commits without validation requests.
	ChangeURI string
}

// GetQueueStatusResult combines a consistent policy lookup with an independent pause observation.
type GetQueueStatusResult struct {
	// Queue is the selected repo-and-ref namespace.
	Queue string
	// CurrentPolicy is the latest applied policy at the lookup's pinned revision.
	CurrentPolicy QueuePolicy
	// PolicyForCommit is the applicable policy when HasPolicyForCommit is true.
	PolicyForCommit QueuePolicy
	// HasPolicyForCommit distinguishes a commit-specific policy from a current-only lookup.
	HasPolicyForCommit bool
	// Execution is an independent observation of the queue-level pause control.
	Execution QueueExecutionStatus
}
