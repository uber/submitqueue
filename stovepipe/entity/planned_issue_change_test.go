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
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlannedIssueChangeValidate(t *testing.T) {
	require.NoError(t, (PlannedIssueChange{}).Validate())
	opening := PlannedIssueChange{Status: IssueStatusOpen, FirstSeenBuildID: "build", TimestampMs: 100}
	require.NoError(t, opening.Validate())
	closing := opening
	closing.Status = IssueStatusClosed
	closing.ExpectedVersion = 7
	require.NoError(t, closing.Validate())
	for _, tt := range []struct {
		name   string
		change func(*PlannedIssueChange)
	}{
		{"missing opening", func(c *PlannedIssueChange) { c.FirstSeenBuildID = "" }},
		{"unknown status", func(c *PlannedIssueChange) { c.Status = IssueStatusUnknown }},
		{"unsupported status", func(c *PlannedIssueChange) { c.Status = "other" }},
		{"negative guard", func(c *PlannedIssueChange) { c.ExpectedVersion = -1 }},
		{"exhausted guard", func(c *PlannedIssueChange) { c.ExpectedVersion = math.MaxInt32 }},
		{"closing absent issue", func(c *PlannedIssueChange) { c.Status = IssueStatusClosed }},
		{"missing timestamp", func(c *PlannedIssueChange) { c.TimestampMs = 0 }},
	} {
		t.Run(tt.name, func(t *testing.T) { value := opening; tt.change(&value); require.Error(t, value.Validate()) })
	}
}

func TestIssueTrackingPlannedChange(t *testing.T) {
	state := IssueTrackingState{RequestID: "10", BuildID: "build", NotificationID: "notice", CursorTaskID: "task", CursorUnitID: "unit", Phase: IssueTrackingPhaseRefreshing, Version: 1,
		PlannedChange: PlannedIssueChange{Status: IssueStatusOpen, FirstSeenBuildID: "build", TimestampMs: 100}}
	require.True(t, state.HasPlannedChange())
	require.NoError(t, state.Validate())
	for _, tt := range []struct {
		name   string
		change func(*IssueTrackingState)
	}{
		{"completed pass", func(s *IssueTrackingState) { s.Phase = IssueTrackingPhaseRefreshed }},
		{"finalized pass", func(s *IssueTrackingState) { s.Phase = IssueTrackingPhaseFinalized }},
		{"missing cursor", func(s *IssueTrackingState) { s.CursorTaskID, s.CursorUnitID = "", "" }},
		{"opening from another build", func(s *IssueTrackingState) { s.PlannedChange.FirstSeenBuildID = "other" }},
		{"refresh cannot close", func(s *IssueTrackingState) {
			s.PlannedChange.Status = IssueStatusClosed
			s.PlannedChange.ExpectedVersion = 1
		}},
	} {
		t.Run(tt.name, func(t *testing.T) { value := state; tt.change(&value); require.Error(t, value.Validate()) })
	}
	state.Phase = IssueTrackingPhaseFinalizing
	state.PlannedChange.Status = IssueStatusClosed
	state.PlannedChange.ExpectedVersion = 7
	state.PlannedChange.FirstSeenBuildID = "older"
	require.NoError(t, state.Validate())
	state.CursorUnitID = ""
	require.NoError(t, state.Validate())

	state.PlannedChange = PlannedIssueChange{}
	require.False(t, state.HasPlannedChange())
	require.Equal(t, "task", state.CursorTaskID)
	state.Phase = IssueTrackingPhaseFinalized
	require.NoError(t, state.Validate())
}
