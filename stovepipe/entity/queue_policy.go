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

// QueuePolicyState describes the deployment-validation requirement for a commit interval.
type QueuePolicyState string

const (
	// QueuePolicyStateUnknown is an unspecified policy, never an explicit disable.
	QueuePolicyStateUnknown QueuePolicyState = ""
	// QueuePolicyStateEnabled requires the Stovepipe validation workflow.
	QueuePolicyStateEnabled QueuePolicyState = "enabled"
	// QueuePolicyStateDisabled has no Stovepipe validation requirement.
	QueuePolicyStateDisabled QueuePolicyState = "disabled"
)

// QueuePolicy is an immutable applied policy covering one commit interval.
type QueuePolicy struct {
	// Revision is the positive queue-scoped ordinal of the applied policy change.
	Revision int64
	// State is the validation requirement, independent of execution and verdicts.
	State QueuePolicyState
	// EffectiveFromCommitURI is the first covered commit, inclusive. Only the initial disabled policy has no boundary.
	EffectiveFromCommitURI string
	// ChangedAtMs is the applied-policy timestamp in Unix milliseconds.
	ChangedAtMs int64
}

// QueuePolicyHead is a versioned pointer to a queue's latest committed policy transition.
type QueuePolicyHead struct {
	// Queue is the stable repo-and-ref namespace.
	Queue string
	// TransitionID identifies the immutable latest transition within the queue.
	TransitionID string
	// Revision is the policy revision represented by TransitionID.
	Revision int64
	// Version is the positive optimistic-locking version of this pointer.
	Version int64
}

// QueuePolicyTransition is an immutable policy occurrence linked to its predecessor.
type QueuePolicyTransition struct {
	// ID is the queue-scoped idempotency key for this occurrence.
	ID string
	// Queue is the stable repo-and-ref namespace.
	Queue string
	// PreviousID identifies the preceding committed occurrence. Empty only for the initial policy.
	PreviousID string
	// Policy is the immutable state and commit boundary of this occurrence.
	Policy QueuePolicy
}

// ApplyQueuePolicyRequest identifies one optimistic, idempotent policy change.
type ApplyQueuePolicyRequest struct {
	// Queue is the stable repo-and-ref namespace.
	Queue string
	// OperationID is the caller-owned queue-scoped idempotency key.
	OperationID string
	// ExpectedRevision is the positive policy revision on which the change is based.
	ExpectedRevision int64
	// State is the new enabled or disabled validation requirement.
	State QueuePolicyState
	// EffectiveFromCommitURI is the explicitly selected, inclusive commit boundary.
	EffectiveFromCommitURI string
}
