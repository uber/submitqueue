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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/core/requestissue"
	"github.com/uber/submitqueue/stovepipe/core/requestlog"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func testRequestIssueProjection(t *testing.T, ctx context.Context, db *sql.DB, factory storage.Factory) {
	t.Helper()
	const queue = "request-issues/main"
	stores, err := factory.For(storage.Config{QueueName: queue})
	require.NoError(t, err)
	projection := stores.GetRequestIssueSummaryStore()
	summary := entity.RequestIssueSummary{RequestID: "7", BuildID: "build/b", TaskID: "test", UnitID: "unit", Version: 1}
	for _, buildID := range []string{"build/a", "build/b"} {
		require.NoError(t, stores.GetBuildIssueStore().Create(ctx, buildID, entity.BuildIssue{TaskID: "test", UnitID: "unit", FixID: "unlogged-association"}))
		log := requestlog.NewRequestEventLog(entity.Request{ID: "7", Queue: queue}, entity.RequestEventBuildTriggered, buildID, map[string]string{requestlog.MetadataKeyBuildID: buildID})
		log.TimestampMs = 1000
		require.NoError(t, stores.GetRequestLogStore().Create(ctx, log))
	}
	repair := requestlog.NewRequestEventLog(entity.Request{ID: "7", Queue: queue}, entity.RequestEventFixSubmitted, "fix/1", map[string]string{
		requestlog.MetadataKeyBuildID: summary.BuildID, requestlog.MetadataKeyTaskID: summary.TaskID, requestlog.MetadataKeyUnitID: summary.UnitID, requestlog.MetadataKeyFixID: "fix/1",
	})
	require.NoError(t, requestlog.NewMaterializer(tally.NoopScope).PersistLog(ctx, stores, repair))
	for range 2 {
		require.NoError(t, requestlog.RebuildRequestIssues(ctx, stores, queue, "7"))
	}
	rows, err := projection.List(ctx, "7", storage.RequestIssueRange{Limit: 1})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "build/a", rows[0].BuildID)
	require.Empty(t, rows[0].FixID)
	rows, err = projection.List(ctx, "7", storage.RequestIssueRange{After: storage.RequestIssueCursor{BuildID: "build/a", TaskID: "test", UnitID: "unit"}, Limit: 2})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "fix/1", rows[0].FixID)
	require.Equal(t, int32(1), rows[0].Version)
	require.ErrorIs(t, projection.Create(ctx, summary), storage.ErrAlreadyExists)
	require.ErrorIs(t, projection.Update(ctx, summary, 5, 6), storage.ErrVersionMismatch)
	summary.FixID = "fix/1"
	require.NoError(t, projection.Update(ctx, summary, 1, 2))
	got, err := projection.Get(ctx, "7", summary.BuildID, summary.TaskID, summary.UnitID)
	require.NoError(t, err)
	require.Equal(t, int32(2), got.Version)
	require.Equal(t, int32(1), summary.Version)
	for _, selection := range []struct{ queue, request string }{{queue, "8"}, {"Request-issues/main", "7"}} {
		other, err := factory.For(storage.Config{QueueName: selection.queue})
		require.NoError(t, err)
		isolated := summary
		isolated.RequestID = selection.request
		require.NoError(t, other.GetRequestIssueSummaryStore().Create(ctx, isolated))
		page, err := other.GetRequestIssueSummaryStore().List(ctx, selection.request, storage.RequestIssueRange{Limit: 10})
		require.NoError(t, err)
		require.Equal(t, []entity.RequestIssueSummary{isolated}, page)
	}
	concurrent := summary
	concurrent.RequestID = "9"
	concurrent.FixID = ""
	require.NoError(t, projection.Create(ctx, concurrent))
	concurrent.FixID = "fix/concurrent"
	results := make(chan error, 2)
	start := make(chan struct{})
	for range 2 {
		go func() { <-start; results <- requestissue.MaterializeObservation(ctx, projection, concurrent) }()
	}
	close(start)
	for range 2 {
		require.NoError(t, <-results)
	}
	merged, err := projection.Get(ctx, "9", concurrent.BuildID, concurrent.TaskID, concurrent.UnitID)
	require.NoError(t, err)
	require.Equal(t, "fix/concurrent", merged.FixID)
	require.Equal(t, int32(2), merged.Version)
	widest := entity.RequestIssueSummary{RequestID: "7", BuildID: strings.Repeat("😀", 255), TaskID: strings.Repeat("t", 255), UnitID: strings.Repeat("😀", 256), FixID: strings.Repeat("😀", 255), Version: 1}
	require.NoError(t, projection.Create(ctx, widest))
	got, err = projection.Get(ctx, widest.RequestID, widest.BuildID, widest.TaskID, widest.UnitID)
	require.NoError(t, err)
	require.Equal(t, widest, got)
	for _, unit := range []string{"", "Unit", "unit ", "a\x00b"} {
		value := summary
		value.UnitID = unit
		require.NoError(t, projection.Create(ctx, value))
	}
	page, err := projection.List(ctx, "7", storage.RequestIssueRange{Limit: 20})
	require.NoError(t, err)
	require.Len(t, page, 7)
	require.Equal(t, "", page[1].UnitID)
	_, err = db.ExecContext(ctx, "DELETE FROM build_issue WHERE queue = ?", queue)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "DELETE FROM request_log WHERE queue = ?", queue)
	require.NoError(t, err)
	afterDeletion, err := projection.List(ctx, "7", storage.RequestIssueRange{Limit: 20})
	require.NoError(t, err)
	require.Equal(t, page, afterDeletion)
	conflicting := summary
	conflicting.FixID = "different-repair"
	require.Error(t, requestissue.MaterializeObservation(ctx, projection, conflicting))
}
