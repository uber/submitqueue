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
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

func TestActiveRequestStore(t *testing.T) {
	failure := errors.New("unavailable")
	for _, tt := range []struct {
		name        string
		setup       func(sqlmock.Sqlmock)
		operation   func(storage.ActiveRequestStore) error
		wantErr     error
		wantFailure bool
	}{
		{name: "create", setup: func(m sqlmock.Sqlmock) {
			m.ExpectExec("INSERT INTO active_request").WithArgs("q", "42").WillReturnResult(sqlmock.NewResult(0, 1))
		}, operation: func(s storage.ActiveRequestStore) error { return s.Create(context.Background(), "42") }},
		{name: "duplicate", setup: func(m sqlmock.Sqlmock) {
			m.ExpectExec("INSERT INTO active_request").WithArgs("q", "42").WillReturnError(&mysql.MySQLError{Number: 1062})
		}, operation: func(s storage.ActiveRequestStore) error { return s.Create(context.Background(), "42") }, wantErr: storage.ErrAlreadyExists},
		{name: "create failure", setup: func(m sqlmock.Sqlmock) { m.ExpectExec("INSERT INTO active_request").WillReturnError(failure) }, operation: func(s storage.ActiveRequestStore) error { return s.Create(context.Background(), "42") }, wantErr: failure},
		{name: "empty identity", operation: func(s storage.ActiveRequestStore) error { return s.Create(context.Background(), "") }, wantFailure: true},
		{name: "list ordered page", setup: func(m sqlmock.Sqlmock) {
			m.ExpectQuery("SELECT request_id FROM active_request").WithArgs("q", "10", 2).WillReturnRows(sqlmock.NewRows([]string{"request_id"}).AddRow("2").AddRow("42"))
		}, operation: func(s storage.ActiveRequestStore) error {
			ids, err := s.List(context.Background(), "10", 2)
			require.Equal(t, []string{"2", "42"}, ids)
			return err
		}},
		{name: "list empty", setup: func(m sqlmock.Sqlmock) {
			m.ExpectQuery("SELECT request_id FROM active_request").WithArgs("q", "", 2).WillReturnRows(sqlmock.NewRows([]string{"request_id"}))
		}, operation: func(s storage.ActiveRequestStore) error {
			ids, err := s.List(context.Background(), "", 2)
			require.Empty(t, ids)
			return err
		}},
		{name: "list failure", setup: func(m sqlmock.Sqlmock) {
			m.ExpectQuery("SELECT request_id FROM active_request").WillReturnError(failure)
		}, operation: func(s storage.ActiveRequestStore) error { _, err := s.List(context.Background(), "", 2); return err }, wantErr: failure},
		{name: "list row failure", setup: func(m sqlmock.Sqlmock) {
			m.ExpectQuery("SELECT request_id FROM active_request").WillReturnRows(sqlmock.NewRows([]string{"request_id"}).AddRow("42").RowError(0, failure))
		}, operation: func(s storage.ActiveRequestStore) error { _, err := s.List(context.Background(), "", 2); return err }, wantErr: failure},
		{name: "invalid limit", operation: func(s storage.ActiveRequestStore) error { _, err := s.List(context.Background(), "", 0); return err }, wantFailure: true},
		{name: "delete", setup: func(m sqlmock.Sqlmock) {
			m.ExpectExec("DELETE FROM active_request").WithArgs("q", "42").WillReturnResult(sqlmock.NewResult(0, 1))
		}, operation: func(s storage.ActiveRequestStore) error { return s.Delete(context.Background(), "42") }},
		{name: "delete absent", setup: func(m sqlmock.Sqlmock) {
			m.ExpectExec("DELETE FROM active_request").WithArgs("q", "42").WillReturnResult(sqlmock.NewResult(0, 0))
		}, operation: func(s storage.ActiveRequestStore) error { return s.Delete(context.Background(), "42") }},
		{name: "delete failure", setup: func(m sqlmock.Sqlmock) { m.ExpectExec("DELETE FROM active_request").WillReturnError(failure) }, operation: func(s storage.ActiveRequestStore) error { return s.Delete(context.Background(), "42") }, wantErr: failure},
		{name: "delete empty identity", operation: func(s storage.ActiveRequestStore) error { return s.Delete(context.Background(), "") }, wantFailure: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			if tt.setup != nil {
				tt.setup(mock)
			}
			err = tt.operation(NewActiveRequestStore(db, testMetrics(), "q"))
			if tt.wantFailure {
				require.Error(t, err)
			} else {
				require.ErrorIs(t, err, tt.wantErr)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
