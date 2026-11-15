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

func TestProjectIssueAttributionValidate(t *testing.T) {
	for _, tt := range []struct {
		name    string
		entry   ProjectIssueAttribution
		wantErr bool
	}{
		{name: "affected project", entry: ProjectIssueAttribution{TaskID: "test", UnitID: "//example:test", Project: "service"}},
		{name: "missing project", entry: ProjectIssueAttribution{TaskID: "test", UnitID: "//example:test"}, wantErr: true},
		{name: "missing task", entry: ProjectIssueAttribution{UnitID: "//example:test", Project: "service"}, wantErr: true},
		{name: "task-wide failure", entry: ProjectIssueAttribution{TaskID: "test", Project: "service"}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantErr {
				require.Error(t, tt.entry.Validate())
			} else {
				require.NoError(t, tt.entry.Validate())
			}
		})
	}
}
