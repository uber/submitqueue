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
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestCounterNext(t *testing.T) {
	const query = "INSERT INTO counter (owner_domain, queue, resource_kind, value) VALUES (?, ?, ?, LAST_INSERT_ID(1)) ON DUPLICATE KEY UPDATE value = LAST_INSERT_ID(value + 1)"

	tests := []struct {
		name    string
		result  int64
		execErr error
		wantErr bool
	}{
		{name: "returns allocated value", result: 42},
		{name: "rejects zero", wantErr: true},
		{name: "propagates increment failure", execErr: fmt.Errorf("database unavailable"), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)

			expectation := mock.ExpectExec(query).WithArgs("submitqueue", "demo-queue", "request")
			if tt.execErr != nil {
				expectation.WillReturnError(tt.execErr)
			} else {
				expectation.WillReturnResult(sqlmock.NewResult(tt.result, 1))
			}
			mock.ExpectClose()

			counter := NewCounter(db, tally.NoopScope, "submitqueue", "demo-queue")
			got, err := counter.Next(context.Background(), "request")
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.result, got)
			}
			require.NoError(t, db.Close())
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
