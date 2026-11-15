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
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/core/issuelog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func testRequestIssueSummaryMaterializer(t *testing.T, ctx context.Context, db *sql.DB, factory storage.Factory) {
	const queue = "request-issue-summary-history"
	bound, err := factory.For(storage.Config{QueueName: queue})
	require.NoError(t, err)
	for _, build := range []entity.Build{
		{ID: "b/12", RequestID: "12", Status: entity.BuildStatusFailed, Version: 1},
		{ID: "b/13", RequestID: "13", Status: entity.BuildStatusFailed, Version: 1},
		{ID: "b/14", RequestID: "14", Status: entity.BuildStatusSucceeded, Version: 1},
	} {
		require.NoError(t, bound.GetBuildStore().Create(ctx, build))
	}
	m := issuelog.NewMaterializer(tally.NoopScope)
	for _, log := range []entity.IssueLog{
		{TaskID: "test", BuildID: "b/12", Event: entity.IssueEventOpened, TimestampMs: 1000},
		{TaskID: "test", BuildID: "b/13", Event: entity.IssueEventReported},
		{TaskID: "test", BuildID: "b/14", Event: entity.IssueEventClosed, TimestampMs: 3000},
	} {
		require.NoError(t, m.PersistLog(ctx, bound, log, "b/12"))
	}
	results := bound.GetRequestIssueSummaryStore()
	for request, kind := range map[string]entity.RequestIssueKind{"12": entity.RequestIssueKindNew, "13": entity.RequestIssueKindExisting, "14": entity.RequestIssueKindClosed} {
		got, err := results.Get(ctx, request, "test", "")
		require.NoError(t, err)
		require.Equal(t, kind, got.Kind)
		require.Equal(t, "b/12", got.FirstSeenBuildID)
	}
	retained, err := bound.GetIssueLogStore().Get(ctx, "test", "", "b/13", entity.IssueEventReported)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `DELETE FROM request_issue_summary WHERE queue = ? AND request_id = ? AND task_id = ? AND unit_id = ?`, queue, "13", "test", "")
	require.NoError(t, err)
	require.NoError(t, m.PersistLog(ctx, bound, retained, retained.FirstSeenBuildID))
	rebuilt, err := results.Get(ctx, "13", "test", "")
	require.NoError(t, err)
	require.Equal(t, entity.RequestIssueKindExisting, rebuilt.Kind)
	global, err := bound.GetIssueSummaryStore().Get(ctx, "test", "")
	require.NoError(t, err)
	require.Equal(t, entity.IssueStatusClosed, global.Status)
	require.Equal(t, int64(3000), global.ChangedAtMs)
	_, err = bound.GetOpenIssueStore().Get(ctx, "test", "")
	require.ErrorIs(t, err, storage.ErrNotFound)
}
