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

// QueueSummary is the current read model derived from retained queue history.
type QueueSummary struct {
	// Queue is the stable repo-and-ref namespace, at most 255 bytes.
	Queue string
	// State is the latest materialized enabled/disabled value.
	State QueueEnablement
	// QueueStateVersion identifies the applied enablement occurrence represented here.
	QueueStateVersion int64
	// EffectiveFromCommitURI is the inclusive immutable boundary; empty at state version 1.
	EffectiveFromCommitURI string
	// StateTimestampMs is the occurrence timestamp in Unix milliseconds.
	StateTimestampMs int64
	// Version is the positive optimistic-lock version of this projection.
	Version int64
}

func (s QueueSummary) Snapshot() QueueState {
	return QueueState{Queue: s.Queue, State: s.State, Version: s.QueueStateVersion,
		EffectiveFromCommitURI: s.EffectiveFromCommitURI, ChangedAtMs: s.StateTimestampMs}
}

func (s QueueSummary) Validate() error {
	if s.Version <= 0 {
		return fmt.Errorf("invalid queue summary version")
	}
	return s.Snapshot().Validate()
}
