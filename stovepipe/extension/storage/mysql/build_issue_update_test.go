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
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func TestBuildIssueUpdate(t *testing.T) {
	for _, tt := range []struct {
		name     string
		affected int64
		execErr  error
		rowsErr  error
		wantErr  error
	}{
		{name: "updated", affected: 1},
		{name: "stale or missing", wantErr: storage.ErrVersionMismatch},
		{name: "write failure", execErr: assert.AnError, wantErr: assert.AnError},
		{name: "rows affected failure", rowsErr: assert.AnError, wantErr: assert.AnError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			issue := entity.BuildIssue{TaskID: "Task ", UnitID: "Unit ", FixID: "Fix/1 ", Version: 99}
			original := issue
			expectation := mock.ExpectExec(regexp.QuoteMeta(`UPDATE build_issue SET fix_id = ?, version = ?
                WHERE queue = ? AND build_id = ? AND task_id = ? AND unit_id = ? AND version = ?`)).
				WithArgs(issue.FixID, int32(2), "Queue ", "Build ", issue.TaskID, issue.UnitID, int32(1))
			switch {
			case tt.execErr != nil:
				expectation.WillReturnError(tt.execErr)
			case tt.rowsErr != nil:
				expectation.WillReturnResult(sqlmock.NewErrorResult(tt.rowsErr))
			default:
				expectation.WillReturnResult(sqlmock.NewResult(0, tt.affected))
			}
			err = NewBuildIssueStore(db, testMetrics(), "Queue ").Update(context.Background(), "Build ", issue, 1, 2)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, original, issue)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestBuildIssueUpdateRejectsMissingIdentity(t *testing.T) {
	for _, key := range []struct{ buildID, taskID string }{{taskID: "task"}, {buildID: "build"}} {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		store := NewBuildIssueStore(db, testMetrics(), "queue")
		require.Error(t, store.Update(context.Background(), key.buildID, entity.BuildIssue{TaskID: key.taskID}, 0, 1))
		require.NoError(t, mock.ExpectationsWereMet())
		mock.ExpectClose()
		require.NoError(t, db.Close())
	}
}
