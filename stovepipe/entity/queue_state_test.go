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

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueueStateInvariants(t *testing.T) {
	initial := QueueState{Queue: "repo/main", State: QueueEnablementDisabled, ChangedAtMs: 1000, Version: 1}
	for _, tc := range []struct {
		name   string
		change func(*QueueState)
		valid  bool
	}{
		{"initial", func(*QueueState) {}, true},
		{"enabled occurrence", func(s *QueueState) { s.Version = 2; s.State = QueueEnablementEnabled; s.EffectiveFromCommitURI = "A" }, true},
		{"missing queue", func(s *QueueState) { s.Queue = "" }, false},
		{"oversized queue", func(s *QueueState) { s.Queue = strings.Repeat("x", 256) }, false},
		{"unknown state", func(s *QueueState) { s.State = QueueEnablementUnknown }, false},
		{"invalid version", func(s *QueueState) { s.Version = 0 }, false},
		{"missing timestamp", func(s *QueueState) { s.ChangedAtMs = 0 }, false},
		{"enabled initial", func(s *QueueState) { s.State = QueueEnablementEnabled }, false},
		{"initial boundary", func(s *QueueState) { s.EffectiveFromCommitURI = "A" }, false},
		{"missing later boundary", func(s *QueueState) { s.Version = 2 }, false},
		{"oversized boundary", func(s *QueueState) { s.Version = 2; s.EffectiveFromCommitURI = strings.Repeat("x", 256) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := initial
			tc.change(&state)
			if tc.valid {
				require.NoError(t, state.Validate())
			} else {
				require.Error(t, state.Validate())
			}
		})
	}
	log := QueueLog{ID: QueueLogID(1), Queue: initial.Queue, TimestampMs: initial.ChangedAtMs, State: initial.State, QueueStateVersion: 1}
	require.NoError(t, log.Validate())
	require.Equal(t, initial, log.Snapshot())
	log.ID = "other"
	require.Error(t, log.Validate())
}
