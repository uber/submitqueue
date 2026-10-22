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

import (
	"fmt"
	"slices"
)

// Queue holds per-queue pipeline coordination state for a named repo+ref (e.g. "monorepo/main").
type Queue struct {
	// ****************
	// Immutable fields, fixed at queue entity creation
	// ****************

	// Name is the stable logical id for the queue. It should be unique within the system.
	Name string `json:"name"`

	// ****************
	// Following fields could be changed throughout the lifecycle of the queue
	// ****************

	// LastGreenURI is the newest head with a successful build outcome, usable as an
	// incremental baseline before validation facts and Git promotion are complete.
	LastGreenURI string `json:"last_green_uri"`

	// LastGreenRequestID is the request that established LastGreenURI. The bookmark only
	// moves forward: a green outcome adopts the pair only when this id is empty or older
	// than the candidate's, compared by ingest order via CompareRequestID.
	LastGreenRequestID string `json:"last_green_request_id"`

	// InFlightCount is the number of reserved build slots, equal to len(InFlightRequestIDs).
	InFlightCount int32 `json:"in_flight_count"`
	// InFlightRequestIDs identifies the requests holding reserved build slots.
	InFlightRequestIDs []string `json:"in_flight_request_ids"`

	// LatestRequestID is the request id of the newest head ingest accepted for this queue.
	// Empty until the first request is created. Coalescing compares IDs via CompareRequestID.
	LatestRequestID string `json:"latest_request_id"`

	// Enablement is the applied enabled/disabled state; empty means unregistered.
	Enablement QueueEnablement `json:"enablement"`

	// EnablementVersion is the queue-scoped enablement sequence; zero means unregistered.
	// Operational changes do not advance this sequence.
	EnablementVersion int64 `json:"enablement_version"`

	// EffectiveFromCommitURI is the inclusive immutable enablement boundary; empty for initial registration.
	EffectiveFromCommitURI string `json:"effective_from_commit_uri"`

	// EnablementChangedAtMs is the enablement application timestamp in Unix milliseconds.
	EnablementChangedAtMs int64 `json:"enablement_changed_at_ms"`

	// Version is the version of the object. It is used for optimistic locking.
	// Versioning starts at 1 and is incremented for each change to the object.
	Version int32 `json:"version"`
}

// HasBuildSlot reports whether requestID owns a reserved build slot.
func (q Queue) HasBuildSlot(requestID string) bool {
	return slices.Contains(q.InFlightRequestIDs, requestID)
}

// ValidateBuildSlots checks that the slot count and ownership agree.
func (q Queue) ValidateBuildSlots() error {
	if q.InFlightCount != int32(len(q.InFlightRequestIDs)) {
		return fmt.Errorf("queue %q has %d slots but %d owners", q.Name, q.InFlightCount, len(q.InFlightRequestIDs))
	}
	seen := make(map[string]struct{}, len(q.InFlightRequestIDs))
	for _, id := range q.InFlightRequestIDs {
		if id == "" {
			return fmt.Errorf("queue %q has an empty slot owner", q.Name)
		}
		if _, exists := seen[id]; exists {
			return fmt.Errorf("queue %q has duplicate slot owner %q", q.Name, id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

func (q Queue) EnablementState() QueueState {
	return QueueState{Queue: q.Name, State: q.Enablement, Version: q.EnablementVersion,
		EffectiveFromCommitURI: q.EffectiveFromCommitURI, ChangedAtMs: q.EnablementChangedAtMs}
}
