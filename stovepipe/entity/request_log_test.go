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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestLogValidate(t *testing.T) {
	base := RequestLog{
		ID:             "state/1",
		Queue:          "monorepo/main",
		RequestID:      "request/monorepo/main/1",
		TimestampMs:    1735689600000,
		State:          RequestStateAccepted,
		RequestVersion: 1,
	}

	tests := []struct {
		name    string
		mutate  func(RequestLog) RequestLog
		wantErr bool
	}{
		{name: "accepted state"},
		{
			name: "superseded state",
			mutate: func(entry RequestLog) RequestLog {
				entry.State = RequestStateSuperseded
				entry.Metadata = map[string]string{"superseded_by_request_id": "request/monorepo/main/2"}
				entry.OutcomeReason = RequestOutcomeReasonSupersededByNewerHead
				return entry
			},
		},
		{
			name: "failed without build",
			mutate: func(entry RequestLog) RequestLog {
				entry.State = RequestStateFailed
				entry.OutcomeReason = RequestOutcomeReasonProcessingFailed
				return entry
			},
		},
		{
			name: "succeeded state",
			mutate: func(entry RequestLog) RequestLog {
				entry.State = RequestStateSucceeded
				entry.Metadata = map[string]string{"build_id": "42"}
				entry.OutcomeReason = RequestOutcomeReasonBuildSucceeded
				return entry
			},
		},
		{
			name: "failed build state",
			mutate: func(entry RequestLog) RequestLog {
				entry.State = RequestStateFailed
				entry.Metadata = map[string]string{"build_id": "42"}
				entry.OutcomeReason = RequestOutcomeReasonBuildFailed
				return entry
			},
		},
		{
			name: "failed polling state",
			mutate: func(entry RequestLog) RequestLog {
				entry.State = RequestStateFailed
				entry.OutcomeReason = RequestOutcomeReasonBuildPollingExhausted
				return entry
			},
		},
		{
			name: "failed timeout state",
			mutate: func(entry RequestLog) RequestLog {
				entry.State = RequestStateFailed
				entry.OutcomeReason = RequestOutcomeReasonValidationTimeout
				return entry
			},
		},
		{
			name: "cancelled state",
			mutate: func(entry RequestLog) RequestLog {
				entry.State = RequestStateCancelled
				entry.Metadata = map[string]string{"build_id": "42"}
				entry.OutcomeReason = RequestOutcomeReasonBuildCancelled
				return entry
			},
		},
		{
			name: "validation fact with green degree",
			mutate: func(entry RequestLog) RequestLog {
				entry.State = RequestStateUnknown
				entry.Event = RequestEventValidationFactRecorded
				entry.RequestVersion = 0
				entry.Metadata = map[string]string{"fact_degree": "0"}
				return entry
			},
		},
		{
			name: "build event",
			mutate: func(entry RequestLog) RequestLog {
				entry.State = RequestStateUnknown
				entry.Event = RequestEventBuildTriggered
				entry.RequestVersion = 0
				entry.Metadata = map[string]string{"build_id": "42"}
				return entry
			},
		},
		{
			name: "build finished event",
			mutate: func(entry RequestLog) RequestLog {
				entry.State = RequestStateUnknown
				entry.Event = RequestEventBuildFinished
				entry.RequestVersion = 0
				entry.Metadata = map[string]string{"build_id": "42"}
				return entry
			},
		},
		{name: "missing ID", mutate: func(entry RequestLog) RequestLog { entry.ID = ""; return entry }, wantErr: true},
		{name: "missing queue", mutate: func(entry RequestLog) RequestLog { entry.Queue = ""; return entry }, wantErr: true},
		{name: "missing request ID", mutate: func(entry RequestLog) RequestLog { entry.RequestID = ""; return entry }, wantErr: true},
		{name: "non-positive timestamp", mutate: func(entry RequestLog) RequestLog { entry.TimestampMs = 0; return entry }, wantErr: true},
		{name: "missing occurrence", mutate: func(entry RequestLog) RequestLog { entry.State = RequestStateUnknown; return entry }, wantErr: true},
		{name: "two occurrences", mutate: func(entry RequestLog) RequestLog {
			entry.Event = RequestEventBuildTriggered
			return entry
		}, wantErr: true},
		{name: "state without version", mutate: func(entry RequestLog) RequestLog { entry.RequestVersion = 0; return entry }, wantErr: true},
		{name: "unknown state", mutate: func(entry RequestLog) RequestLog {
			entry.State = RequestState("future")
			return entry
		}, wantErr: true},
		{name: "non-terminal state with outcome", mutate: func(entry RequestLog) RequestLog {
			entry.OutcomeReason = RequestOutcomeReasonProcessingFailed
			return entry
		}, wantErr: true},
		{name: "opaque metadata", mutate: func(entry RequestLog) RequestLog {
			entry.Metadata = map[string]string{"arbitrary": "value"}
			return entry
		}},
		{
			name: "failed without reason",
			mutate: func(entry RequestLog) RequestLog {
				entry.State = RequestStateFailed
				return entry
			},
			wantErr: true,
		},
		{
			name: "event with request version",
			mutate: func(entry RequestLog) RequestLog {
				entry.State = RequestStateUnknown
				entry.Event = RequestEventBuildTriggered
				entry.Metadata = map[string]string{"build_id": "42"}
				return entry
			},
			wantErr: true,
		},
		{
			name: "unknown event",
			mutate: func(entry RequestLog) RequestLog {
				entry.State = RequestStateUnknown
				entry.Event = RequestEvent("future")
				entry.RequestVersion = 0
				return entry
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := base
			if tt.mutate != nil {
				entry = tt.mutate(entry)
			}
			err := entry.Validate()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
