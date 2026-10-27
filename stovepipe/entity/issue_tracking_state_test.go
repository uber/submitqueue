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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIssueTrackingState(t *testing.T) {
	for _, phase := range []IssueTrackingPhase{IssueTrackingPhaseRefreshing, IssueTrackingPhaseRefreshed, IssueTrackingPhaseFinalizing, IssueTrackingPhaseFinalized} {
		t.Run(string(phase), func(t *testing.T) {
			state := IssueTrackingState{RequestID: "10", BuildID: "build/10", NotificationID: "notification/10", Phase: phase, Version: 1}
			require.NoError(t, state.Validate())
		})
	}
	for _, tt := range []struct {
		name   string
		change func(*IssueTrackingState)
	}{
		{"missing request", func(s *IssueTrackingState) { s.RequestID = "" }},
		{"noncanonical request", func(s *IssueTrackingState) { s.RequestID = "010" }},
		{"nonnumeric request", func(s *IssueTrackingState) { s.RequestID = "request/10" }},
		{"missing notification", func(s *IssueTrackingState) { s.NotificationID = "" }},
		{"unit cursor without task", func(s *IssueTrackingState) { s.CursorUnitID = "unit" }},
		{"missing build", func(s *IssueTrackingState) { s.BuildID = "" }},
		{"unknown phase", func(s *IssueTrackingState) { s.Phase = IssueTrackingPhaseUnknown }},
		{"unsupported phase", func(s *IssueTrackingState) { s.Phase = "other" }},
		{"zero version", func(s *IssueTrackingState) { s.Version = 0 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			state := IssueTrackingState{RequestID: "10", BuildID: "build/10", NotificationID: "notification/10", Phase: IssueTrackingPhaseRefreshing, Version: 1}
			tt.change(&state)
			require.Error(t, state.Validate())
		})
	}
}
