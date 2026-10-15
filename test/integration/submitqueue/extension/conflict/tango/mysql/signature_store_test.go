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

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/uber/submitqueue/submitqueue/extension/conflict/tango"
	tangomysql "github.com/uber/submitqueue/submitqueue/extension/conflict/tango/mysql"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
	"github.com/uber/submitqueue/test/testutil"
)

// SignatureStoreIntegrationSuite runs the MySQL signature store against a real
// database.
type SignatureStoreIntegrationSuite struct {
	suite.Suite
	stack *testutil.ComposeStack
	db    *sql.DB
	log   *testutil.TestLogger
}

func TestSignatureStoreIntegration(t *testing.T) {
	suite.Run(t, new(SignatureStoreIntegrationSuite))
}

func (s *SignatureStoreIntegrationSuite) SetupSuite() {
	t := s.T()
	ctx := context.Background()
	s.log = testutil.NewTestLogger(t)

	s.stack = testutil.NewComposeStack(t, s.log, ctx, "docker-compose.yml", "ext-submitqueue-conflict-tango-mysql")
	require.NoError(t, s.stack.Up(), "failed to start compose stack")

	var err error
	s.db, err = s.stack.ConnectMySQLService("mysql")
	require.NoError(t, err, "failed to connect to MySQL")
	testutil.ApplySchema(t, s.log, s.db, testutil.SchemaDir("submitqueue/extension/conflict/tango/mysql/schema"))

	t.Cleanup(func() { s.db.Close() })
}

func (s *SignatureStoreIntegrationSuite) forQueue(queue string) tango.SignatureStore {
	return tangomysql.NewSignatureStore(s.db, tally.NoopScope, queue)
}

func (s *SignatureStoreIntegrationSuite) TestSignatureStore() {
	t := s.T()
	ctx := context.Background()
	storeA := s.forQueue("sig-queue-a")
	storeB := s.forQueue("sig-queue-b")

	// 100k entries encode to roughly 900KB, beyond a BLOB column's 64KB.
	large := make([]tango.SignatureTarget, 100000)
	for i := range large {
		large[i] = tango.SignatureTarget{ID: uint64(i) * 3, Distance: int32(i % 5)}
	}

	tests := []struct {
		name      string
		signature tango.TargetSignature
	}{
		{
			name: "several targets",
			signature: tango.TargetSignature{
				Queue: "sig-queue-a", BatchID: "small", BaseRevision: "rev-1", CreatedAt: 1700000000000,
				Targets: []tango.SignatureTarget{{ID: 1, Distance: 0}, {ID: 1 << 63, Distance: 2}, {ID: ^uint64(0), Distance: 7}},
			},
		},
		{
			name:      "empty signature",
			signature: tango.TargetSignature{Queue: "sig-queue-a", BatchID: "empty", BaseRevision: "rev-1", CreatedAt: 1},
		},
		{
			name:      "large signature",
			signature: tango.TargetSignature{Queue: "sig-queue-a", BatchID: "large", BaseRevision: "rev-2", CreatedAt: 2, Targets: large},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, storeA.Create(ctx, tt.signature))

			got, found, err := storeA.Get(ctx, tt.signature.BatchID)
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, tt.signature, got)
		})
	}

	t.Run("missing batch is not found", func(t *testing.T) {
		_, found, err := storeA.Get(ctx, "missing")
		require.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("second create keeps the first signature", func(t *testing.T) {
		duplicate := tango.TargetSignature{Queue: "sig-queue-a", BatchID: "small", BaseRevision: "rev-other", CreatedAt: 9}
		require.ErrorIs(t, storeA.Create(ctx, duplicate), storage.ErrAlreadyExists)

		got, found, err := storeA.Get(ctx, "small")
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, "rev-1", got.BaseRevision)
	})

	t.Run("invisible through another queue", func(t *testing.T) {
		_, found, err := storeB.Get(ctx, "small")
		require.NoError(t, err)
		assert.False(t, found)
		require.Error(t, storeB.Create(ctx, tango.TargetSignature{Queue: "sig-queue-a", BatchID: "mismatch"}), "a mismatched-queue write must be rejected")
	})
}
