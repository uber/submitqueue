// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vitess

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/base/failure"
	entityqueue "github.com/uber/submitqueue/platform/base/messagequeue"
	extqueue "github.com/uber/submitqueue/platform/extension/messagequeue"
	queueMySQL "github.com/uber/submitqueue/platform/extension/messagequeue/mysql"
	queueAdmin "github.com/uber/submitqueue/platform/extension/messagequeue/mysql/ctl/lib"
	"github.com/uber/submitqueue/test/testutil"
	"go.uber.org/zap/zaptest"
)

const (
	keyspace     = "submitqueue"
	shardLower   = "-80"
	shardUpper   = "80-"
	vtgatePort   = 33577
	testTopic    = "vitess_tenant_isolation"
	partitionKey = "shared-partition"
	messageID    = "shared-message"
)

func TestTenantShardingThroughVTGate(t *testing.T) {
	ctx := t.Context()
	log := testutil.NewTestLogger(t)
	stack := testutil.NewComposeStack(
		t,
		log,
		ctx,
		"docker-compose.yml",
		"ext-messagequeue-vitess",
		testutil.WithBuildContext(vitessBuildContext()),
	)
	require.NoError(t, stack.Up())

	vtgate := connectVTGate(t, stack, keyspace)
	lowerShard := connectVTGate(t, stack, keyspace+":"+shardLower)
	upperShard := connectVTGate(t, stack, keyspace+":"+shardUpper)
	shards := map[string]*sql.DB{
		shardLower: lowerShard,
		shardUpper: upperShard,
	}

	tenants := findTenantsOnDifferentShards(t, ctx, vtgate, shards)
	tenantLower := tenants[shardLower]
	tenantUpper := tenants[shardUpper]
	require.NotEqual(t, tenantLower, tenantUpper)

	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           vtgate,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
		Tenants:      []string{tenantLower, tenantUpper},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, q.Close())
	})

	cfg := extqueue.DefaultSubscriptionConfig("vitess-worker", "vitess-consumer")
	cfg.PartitionDiscoveryIntervalMs = 100
	cfg.VisibilityTimeoutMs = cfg.LeaseDurationMs * 10
	deliveries, err := q.Subscriber().Subscribe(ctx, testTopic, cfg)
	require.NoError(t, err)

	for _, tenant := range []string{tenantLower, tenantUpper} {
		msg := entityqueue.NewMessage(messageID, []byte(tenant), partitionKey, nil)
		msg.Tenant = tenant
		require.NoError(t, q.Publisher().Publish(ctx, testTopic, msg))
	}

	received := make(map[string]extqueue.Delivery, 2)
	for len(received) < 2 {
		select {
		case <-ctx.Done():
			require.FailNow(t, "timed out waiting for both tenant deliveries", ctx.Err())
		case delivery, ok := <-deliveries:
			require.True(t, ok)
			require.NotNil(t, delivery)
			received[delivery.Message().Tenant] = delivery
		}
	}
	assert.ElementsMatch(t, []string{tenantLower, tenantUpper}, mapKeys(received))

	for _, table := range []string{
		"queue_messages",
		"queue_delivery_state",
		"queue_offsets",
		"queue_partition_leases",
		"queue_subscriber_heartbeats",
	} {
		assertTenantOnOnlyShard(t, ctx, shards, table, tenantLower, shardLower)
		assertTenantOnOnlyShard(t, ctx, shards, table, tenantUpper, shardUpper)
	}

	require.NoError(t, received[tenantLower].Reject(
		ctx,
		failure.New("vitess shard-local DLQ test"),
	))
	assertTopicOnOnlyShard(t, ctx, shards, tenantLower, testTopic+"_dlq", shardLower)
	assert.Zero(t, tenantTopicCount(t, ctx, lowerShard, tenantLower, testTopic))
	assertTopicOnOnlyShard(t, ctx, shards, tenantUpper, testTopic, shardUpper)

	topics, err := queueAdmin.NewAdminStore(vtgate).ListTopics(ctx, queueAdmin.TenantScope{AllTenants: true})
	require.NoError(t, err)
	assert.Contains(t, topics, queueAdmin.TopicInfo{Tenant: tenantLower, Topic: testTopic + "_dlq", MessageCount: 1})
	assert.Contains(t, topics, queueAdmin.TopicInfo{Tenant: tenantUpper, Topic: testTopic, MessageCount: 1})

	require.NoError(t, received[tenantUpper].Ack(ctx))
}

func vitessBuildContext() map[string]string {
	const (
		schemaRoot = "platform/extension/messagequeue/mysql/schema/"
		testRoot   = "test/integration/extension/messagequeue/mysql/vitess/"
	)
	files := map[string]string{
		testRoot + "Dockerfile": testRoot + "Dockerfile",
		"platform/extension/messagequeue/mysql/vitess/vschema.json": "platform/extension/messagequeue/mysql/vitess/vschema.json",
	}
	for _, name := range []string{
		"queue_delivery_state.sql",
		"queue_messages.sql",
		"queue_offsets.sql",
		"queue_partition_leases.sql",
		"queue_subscriber_heartbeats.sql",
	} {
		files[schemaRoot+name] = schemaRoot + name
	}
	return files
}

func connectVTGate(t *testing.T, stack *testutil.ComposeStack, database string) *sql.DB {
	t.Helper()
	port, err := stack.ServicePort("vtcombo", vtgatePort)
	require.NoError(t, err)
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(localhost:%d)/%s?parseTime=true&interpolateParams=true", port, database))
	require.NoError(t, err)
	require.NoError(t, db.Ping())
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	return db
}

func findTenantsOnDifferentShards(
	t *testing.T,
	ctx context.Context,
	vtgate *sql.DB,
	shards map[string]*sql.DB,
) map[string]string {
	t.Helper()
	const probeTopic = "vitess_routing_probe"

	q, err := queueMySQL.NewQueue(queueMySQL.Params{
		DB:           vtgate,
		Logger:       zaptest.NewLogger(t),
		MetricsScope: tally.NoopScope,
	})
	require.NoError(t, err)

	tenantsByShard := make(map[string]string, 2)
	var publishedTenants []string
	for candidate := 0; candidate < 64 && len(tenantsByShard) < len(shards); candidate++ {
		tenant := fmt.Sprintf("vitess-tenant-%d", candidate)
		msg := entityqueue.NewMessage(messageID, []byte(tenant), partitionKey, nil)
		msg.Tenant = tenant
		require.NoError(t, q.Publisher().Publish(ctx, probeTopic, msg))
		publishedTenants = append(publishedTenants, tenant)

		for shard, db := range shards {
			if tenantTopicCount(t, ctx, db, tenant, probeTopic) == 1 {
				tenantsByShard[shard] = tenant
			}
		}
	}
	require.NoError(t, q.Close())
	require.Len(t, tenantsByShard, len(shards))

	for _, tenant := range publishedTenants {
		_, err := vtgate.ExecContext(ctx, "DELETE FROM queue_messages WHERE tenant = ? AND topic = ?", tenant, probeTopic)
		require.NoError(t, err)
	}
	return tenantsByShard
}

func assertTenantOnOnlyShard(
	t *testing.T,
	ctx context.Context,
	shards map[string]*sql.DB,
	table string,
	tenant string,
	expectedShard string,
) {
	t.Helper()
	for shard, db := range shards {
		var count int
		err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE tenant = ?", tenant).Scan(&count)
		require.NoError(t, err)
		if shard == expectedShard {
			require.Positive(t, count, "%s should contain %s on shard %s", table, tenant, shard)
		} else {
			require.Zero(t, count, "%s should not contain %s on shard %s", table, tenant, shard)
		}
	}
}

func assertTopicOnOnlyShard(
	t *testing.T,
	ctx context.Context,
	shards map[string]*sql.DB,
	tenant string,
	topic string,
	expectedShard string,
) {
	t.Helper()
	for shard, db := range shards {
		count := tenantTopicCount(t, ctx, db, tenant, topic)
		if shard == expectedShard {
			require.Equal(t, 1, count)
		} else {
			require.Zero(t, count)
		}
	}
}

func tenantTopicCount(t *testing.T, ctx context.Context, db *sql.DB, tenant string, topic string) int {
	t.Helper()
	var count int
	err := db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM queue_messages WHERE tenant = ? AND topic = ?",
		tenant,
		topic,
	).Scan(&count)
	require.NoError(t, err)
	return count
}

func mapKeys(deliveries map[string]extqueue.Delivery) []string {
	keys := make([]string, 0, len(deliveries))
	for tenant := range deliveries {
		keys = append(keys, tenant)
	}
	return keys
}
