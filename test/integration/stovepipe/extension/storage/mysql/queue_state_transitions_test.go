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
	"github.com/uber/submitqueue/stovepipe/core/queuelog"
	"github.com/uber/submitqueue/stovepipe/core/queuestate"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/sourcecontrol"
	scfake "github.com/uber/submitqueue/stovepipe/extension/sourcecontrol/fake"
	mysqlstorage "github.com/uber/submitqueue/stovepipe/extension/storage/mysql"
)

type transitionSourceFactory struct{ sc sourcecontrol.SourceControl }

func (f *transitionSourceFactory) For(sourcecontrol.Config) (sourcecontrol.SourceControl, error) {
	return f.sc, nil
}

func testQueueStateTransitions(t *testing.T, ctx context.Context, db *sql.DB, backend *mysqlstorage.Storage) {
	t.Helper()
	const queue = "transition/main"
	source := &transitionSourceFactory{sc: scfake.New(sourcecontrol.Config{QueueName: queue}, []string{"A", "before"})}
	manager := queuestate.NewTransitionController(mysqlFactory{backend: backend}, source, queuelog.NewMaterializer(tally.NoopScope))
	_, err := manager.Initialize(ctx, queue)
	require.NoError(t, err)
	enable, err := manager.Prepare(ctx, queue, entity.QueueEnablementEnabled)
	require.NoError(t, err)
	enabled, err := manager.Apply(ctx, enable)
	require.NoError(t, err)
	require.Equal(t, int64(2), enabled.Version)
	// Simulate a crash after the snapshot commit but before history retention.
	_, err = db.ExecContext(ctx, "DELETE FROM queue_log WHERE queue = ? AND log_id = ?", queue, entity.QueueLogID(2))
	require.NoError(t, err)
	source.sc = scfake.New(sourcecontrol.Config{QueueName: queue}, []string{"B", "A", "before"})
	disable, err := manager.Prepare(ctx, queue, entity.QueueEnablementDisabled)
	require.NoError(t, err)
	bound, err := backend.For(queue)
	require.NoError(t, err)
	repaired, err := bound.GetQueueLogStore().Get(ctx, entity.QueueLogID(2))
	require.NoError(t, err)
	require.Equal(t, enabled, repaired.Snapshot())
	disabled, err := manager.Apply(ctx, disable)
	require.NoError(t, err)
	require.Equal(t, int64(3), disabled.Version)
	restarted := queuestate.NewTransitionController(mysqlFactory{backend: backend}, source, queuelog.NewMaterializer(tally.NoopScope))
	replay, err := restarted.Apply(ctx, enable)
	require.NoError(t, err)
	require.Equal(t, enabled, replay)
	current, err := bound.GetQueueSummaryStore().Get(ctx)
	require.NoError(t, err)
	require.Equal(t, disabled, current.Snapshot())
}
