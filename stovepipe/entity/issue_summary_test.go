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

func TestIssueSummaryValidate(t *testing.T) {
	valid := IssueSummary{TaskID: "validation", UnitID: "unit", Status: IssueStatusOpen,
		FirstSeenBuildID: "build/1", RequestID: "1", BuildID: "build/1", ChangedAtMs: 1000, Version: 1}
	tests := []struct {
		name    string
		change  func(*IssueSummary)
		wantErr bool
	}{
		{name: "open"},
		{name: "closed", change: func(s *IssueSummary) { s.Status = IssueStatusClosed }},
		{name: "task wide", change: func(s *IssueSummary) { s.UnitID = "" }},
		{"missing task", func(s *IssueSummary) { s.TaskID = "" }, true},
		{"missing opening", func(s *IssueSummary) { s.FirstSeenBuildID = "" }, true},
		{"missing source build", func(s *IssueSummary) { s.BuildID = "" }, true},
		{"missing source request", func(s *IssueSummary) { s.RequestID = "" }, true},
		{"noncanonical source request", func(s *IssueSummary) { s.RequestID = "01" }, true},
		{"invalid source request", func(s *IssueSummary) { s.RequestID = "build/1" }, true},
		{"unknown status", func(s *IssueSummary) { s.Status = IssueStatusUnknown }, true},
		{"invalid status", func(s *IssueSummary) { s.Status = "other" }, true},
		{"missing timestamp", func(s *IssueSummary) { s.ChangedAtMs = 0 }, true},
		{"negative timestamp", func(s *IssueSummary) { s.ChangedAtMs = -1 }, true},
		{"missing version", func(s *IssueSummary) { s.Version = 0 }, true},
		{"negative version", func(s *IssueSummary) { s.Version = -1 }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			summary := valid
			if tt.change != nil {
				tt.change(&summary)
			}
			if tt.wantErr {
				require.Error(t, summary.Validate())
			} else {
				require.NoError(t, summary.Validate())
			}
		})
	}
}
