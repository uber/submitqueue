// Copyright (c) 2025 Uber Technologies, Inc.
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
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

// testMetrics returns a test metrics scope for use in tests.
func testMetrics() tally.Scope {
	return tally.NoopScope
}

func TestNewStorage(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	s, err := NewStorage(db, testMetrics())
	require.NoError(t, err)

	assert.NotNil(t, s.GetRequestStore())
	assert.NotNil(t, s.GetChangeStore())
	assert.NotNil(t, s.GetBatchStore())
	assert.NotNil(t, s.GetRequestBatchStore())
	assert.NotNil(t, s.GetBatchDependentStore())
	assert.NotNil(t, s.GetBuildStore())
	assert.NotNil(t, s.GetSpeculationPathBuildStore())
	assert.NotNil(t, s.GetSpeculationTreeStore())
	assert.NotNil(t, s.GetRequestLogStore())
	assert.NotNil(t, s.GetRequestSummaryStore())
	assert.NotNil(t, s.GetRequestQueueSummaryStore())
	assert.NotNil(t, s.GetRequestURIStore())
}

func TestMysqlStorage_Close(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	mock.ExpectClose()

	s, err := NewStorage(db, testMetrics())
	require.NoError(t, err)

	require.NoError(t, s.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}
