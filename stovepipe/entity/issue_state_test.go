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

func TestIssueStateValidate(t *testing.T) {
	valid := IssueState{TaskID: "task", FirstSeenBuildID: "build", Status: IssueStatusOpen, Version: 1, LastProcessedBuildID: "build", LastChangedBuildID: "build", ChangedAtMs: 1000}
	for _, status := range []IssueStatus{IssueStatusOpen, IssueStatusClosed} {
		value := valid
		value.Status = status
		require.NoError(t, value.Validate())
	}
	for _, field := range []string{"task", "build", "status", "version", "processed build", "changed build", "timestamp"} {
		t.Run(field, func(t *testing.T) {
			value := valid
			switch field {
			case "task":
				value.TaskID = ""
			case "build":
				value.FirstSeenBuildID = ""
			case "status":
				value.Status = IssueStatusUnknown
			case "processed build":
				value.LastProcessedBuildID = ""
			case "changed build":
				value.LastChangedBuildID = ""
			case "timestamp":
				value.ChangedAtMs = 0
			case "version":
				value.Version = 0
			}
			require.Error(t, value.Validate())
		})
	}
}
