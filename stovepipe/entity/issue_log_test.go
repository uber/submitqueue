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

func TestIssueLogValidate(t *testing.T) {
	for _, tt := range []struct {
		name    string
		change  func(*IssueLog)
		invalid bool
	}{
		{name: "open"},
		{name: "closed", change: func(l *IssueLog) { l.Status = "closed" }},
		{name: "task-wide issue", change: func(l *IssueLog) { l.UnitID = "" }},
		{name: "missing build", change: func(l *IssueLog) { l.BuildID = "" }, invalid: true},
		{name: "zero timestamp", change: func(l *IssueLog) { l.TimestampMs = 0 }, invalid: true},
		{name: "negative timestamp", change: func(l *IssueLog) { l.TimestampMs = -1 }, invalid: true},
		{name: "unknown status", change: func(l *IssueLog) { l.Status = IssueStatusUnknown }, invalid: true},
		{name: "unsupported status", change: func(l *IssueLog) { l.Status = "other" }, invalid: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			log := IssueLog{TaskID: "task", UnitID: "unit",
				Status: IssueStatusOpen, BuildID: "build/1", TimestampMs: 1000}
			if tt.change != nil {
				tt.change(&log)
			}

			if tt.invalid {
				require.Error(t, log.Validate())
			} else {
				require.NoError(t, log.Validate())
			}
		})
	}
}
