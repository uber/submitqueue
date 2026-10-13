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

import "fmt"

// QueueLog is one immutable occurrence of a committed enablement state.
type QueueLog struct {
	// ID is the stable queue-scoped occurrence identity derived from QueueStateVersion.
	ID string
	// Queue is the stable repo-and-ref namespace.
	Queue string
	// TimestampMs is the application's timestamp in Unix milliseconds.
	TimestampMs int64
	// State is the committed enabled or disabled value.
	State QueueEnablement
	// QueueStateVersion is the positive version identifying the applied state.
	QueueStateVersion int64
	// EffectiveFromCommitURI is the inclusive immutable boundary; empty only at version 1.
	EffectiveFromCommitURI string
}

// QueueLogID returns the stable occurrence identity for a state version.
func QueueLogID(version int64) string {
	return fmt.Sprintf("state/%d", version)
}

// Snapshot returns the applied state represented by the occurrence.
func (l QueueLog) Snapshot() QueueState {
	return QueueState{Queue: l.Queue, State: l.State, Version: l.QueueStateVersion,
		EffectiveFromCommitURI: l.EffectiveFromCommitURI, ChangedAtMs: l.TimestampMs}
}

// Validate verifies a retained occurrence's identity and snapshot invariants.
func (l QueueLog) Validate() error {
	if l.ID != QueueLogID(l.QueueStateVersion) {
		return fmt.Errorf("queue state log identity differs from its state version")
	}
	return l.Snapshot().Validate()
}
