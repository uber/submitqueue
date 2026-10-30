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

package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/core/issuelog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func testOpenIssueMaterialization(t *testing.T, ctx context.Context, factory storage.Factory) {
	t.Helper()
	stores, err := factory.For(storage.Config{QueueName: "open-issue-materialization/main"})
	require.NoError(t, err)
	for _, build := range []entity.Build{
		{ID: "build/z", RequestID: "9", Status: entity.BuildStatusFailed, Version: 1},
		{ID: "build/a", RequestID: "10", Status: entity.BuildStatusSucceeded, Version: 1},
		{ID: "build/m", RequestID: "11", Status: entity.BuildStatusFailed, Version: 1},
	} {
		require.NoError(t, stores.GetBuildStore().Create(ctx, build))
	}
	materializer := issuelog.NewMaterializer(tally.NoopScope)
	openIssues := stores.GetOpenIssueStore()
	opened := entity.IssueLog{TaskID: "validation", UnitID: "unit", BuildID: "build/z", Status: entity.IssueStatusOpen, TimestampMs: 1000}
	closed := opened
	closed.BuildID, closed.Status, closed.TimestampMs = "build/a", entity.IssueStatusClosed, 2000
	reopened := opened
	reopened.BuildID, reopened.TimestampMs = "build/m", 3000
	for _, step := range []struct {
		log                      entity.IssueLog
		opening, expectedOpening string
	}{
		{opened, opened.BuildID, opened.BuildID},
		{closed, opened.BuildID, ""},
		{opened, opened.BuildID, ""},
		{reopened, reopened.BuildID, reopened.BuildID},
		{closed, opened.BuildID, reopened.BuildID},
	} {
		require.NoError(t, materializer.PersistLog(ctx, stores, step.log, step.opening))
		refs, err := openIssues.List(ctx, storage.OpenIssueRange{Limit: 2})
		require.NoError(t, err)
		summary, err := stores.GetIssueSummaryStore().Get(ctx, opened.TaskID, opened.UnitID)
		require.NoError(t, err)
		if step.expectedOpening == "" {
			require.Empty(t, refs)
			require.Equal(t, entity.IssueStatusClosed, summary.Status)
		} else {
			require.Equal(t, []entity.OpenIssue{{TaskID: opened.TaskID, UnitID: opened.UnitID, FirstSeenBuildID: step.expectedOpening}}, refs)
			require.Equal(t, entity.IssueStatusOpen, summary.Status)
			require.Equal(t, step.expectedOpening, summary.FirstSeenBuildID)
		}
	}

	current, err := openIssues.Get(ctx, opened.TaskID, opened.UnitID)
	require.NoError(t, err)
	deleted, err := openIssues.Delete(ctx, current)
	require.NoError(t, err)
	require.True(t, deleted)
	require.NoError(t, materializer.PersistLog(ctx, stores, opened, opened.BuildID))
	repaired, err := openIssues.Get(ctx, opened.TaskID, opened.UnitID)
	require.NoError(t, err)
	require.Equal(t, current, repaired)

	stale := current
	stale.FirstSeenBuildID = opened.BuildID
	replaced, err := openIssues.Replace(ctx, stale, current.FirstSeenBuildID)
	require.NoError(t, err)
	require.True(t, replaced)
	require.NoError(t, materializer.PersistLog(ctx, stores, closed, opened.BuildID))
	repaired, err = openIssues.Get(ctx, opened.TaskID, opened.UnitID)
	require.NoError(t, err)
	require.Equal(t, current, repaired)
}
