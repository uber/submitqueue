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

package noop

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/issueattribution"
)

func TestResolveLeavesProjectsUnassessed(t *testing.T) {
	for _, tt := range []struct {
		name   string
		queue  string
		issues []entity.OpenIssue
	}{
		{
			name:  "no issues",
			queue: "validation/main",
		},
		{
			name:  "unit issues",
			queue: "validation/main",
			issues: []entity.OpenIssue{
				{TaskID: "test", UnitID: "//lib:a_test", FirstSeenBuildID: "build/100"},
				{TaskID: "test", UnitID: "//lib:b_test", FirstSeenBuildID: "build/101"},
			},
		},
		{
			name:  "task-wide issue",
			queue: "validation/release",
			issues: []entity.OpenIssue{
				{TaskID: "compile", FirstSeenBuildID: "build/100"},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			resolver, err := New().For(issueattribution.Config{QueueName: tt.queue})
			require.NoError(t, err)

			original := slices.Clone(tt.issues)
			result, err := resolver.Resolve(context.Background(), entity.BuildID{ID: "build/102"}, tt.issues)
			require.NoError(t, err)
			require.Empty(t, result.AssessedProjects)
			require.Empty(t, result.Issues)
			require.Equal(t, original, tt.issues)
		})
	}
}
